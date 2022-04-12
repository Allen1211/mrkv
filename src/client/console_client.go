package client

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/liushuochen/gotable"
	"github.com/liushuochen/gotable/cell"
	table2 "github.com/liushuochen/gotable/table"

	"mrkv/src/client/etc"
	"mrkv/src/common"
	"mrkv/src/master"
	"mrkv/src/replica"
)

type Operation string

const (
	NoOp			= ""
	OpGet 		    = "get"
	OpPut			= "put"
	OpAppend		= "append"
	OpDelete		= "del"
	OpJoin			= "join"
	OpLeave			= "leave"
	OpShow			= "show"
	OpTransfer	    = "trans_leader"

	OpHelp			= "help"
	OpQuit			= "quit"
)

type OpDesc struct {
	argc	int
	retc	int
	usage	string
	desc	string
}

var opMap = map[string]OpDesc{
	NoOp: 		{0, 0, "", ""},
	OpGet: 		{1, 1, "get [key]", "查找键值对"},
	OpPut: 		{2, 0, "put [key] [val]", "插入键值对"},
	OpAppend: 	{2, 0, "append [key] [val]", "对该键进行追加值"},
	OpDelete: 	{1, 0, "del [key] [val]", "删除键值对"},
	OpJoin: 	{2, 0, "join [gid] [node0, node1...]", "加入raft组"},
	OpLeave: 	{1, 0, "leave [gid]", "移除raft组"},
	OpShow: 	{1, 0, "show [master|node|group] id0 id1....idn", "展示集群信息 (master信息, 节点信息, raft组信息, 分片信息)"},
	OpTransfer: {2, 0, "trans_leader [gid] [node]", "转移raft组的Leader到某节点"},
	OpQuit: 	{0, 0, "quit", "结束"},
	OpHelp: 	{0, 0, "help", "获取帮助"},
}


type ConsoleClient struct {
	api		API

	stdin 	*bufio.Scanner
	stdout 	*bufio.Writer
	lineCh	chan string
}

func MakeConsoleClient(conf etc.ClientConf, in *bufio.Scanner, out *bufio.Writer) *ConsoleClient {
	api := MakeMrKVClient(conf.Masters)

	if in == nil {
		in = bufio.NewScanner(os.Stdin)
	}
	if out == nil {
		out = bufio.NewWriter(os.Stdout)
	}

	return &ConsoleClient{
		api: api,
		stdin: in,
		stdout: out,
		lineCh: make(chan string, 100),
	}
}

func (cc *ConsoleClient) Start()  {
	printUserGuide(cc.stdout)

	go cc.inputG()
	cc.outputG()
}

func (cc *ConsoleClient) inputG()  {
	stdout := cc.stdout
	stdin := cc.stdin

	_, _ = stdout.WriteString("\n" + cc.slash())
	_ = stdout.Flush()
	for {
		if !stdin.Scan() {
			continue
		}
		line := stdin.Text()
		if line == "" {
			_, _ = stdout.WriteString(cc.slash())
			_ = stdout.Flush()
			continue
		}
		cc.lineCh <- line
	}
}

func (cc *ConsoleClient) outputG() {
	stdout := cc.stdout

	var err error
	var op 	Operation
	var args []string

	for line := range cc.lineCh {
		// parse input line to request
		if op, args, err = cc.parseInput(line); err != nil {
			_, _ = stdout.WriteString(err.Error())
			_, _ = stdout.WriteString("\n" + cc.slash())
			_ = stdout.Flush()
			continue
		}

		// request, wait for response
		// tc := new(base.TimeCounter)
		// tc.Reset()

		cc.process(op, args)

		// timeCost := tc.CountMilliseconds()

		_ = stdout.Flush()
	}
}

func (cc *ConsoleClient) process(op Operation, args []string)  {
	stdout := cc.stdout

	opDesc := opMap[string(op)]

	if len(args) < opDesc.argc {
		cc.output(
			fmt.Sprintf("not enouch arguments for operation %s, require: %d, given: %d", op, opDesc.argc, len(args)),
			opDesc.usage,
		)
		return
	}

	switch op {

	case NoOp:
		cc.output()
	case OpQuit:
		os.Exit(0)
	case OpHelp:
		printUserGuide(stdout)
		cc.output()

	case OpGet:
		key := args[0]
		reply := cc.api.Get(key)
		if reply.Err == common.OK {
			cc.output(string(reply.Err), string(reply.Value), cc.fromStr(reply.FromReply))
		} else {
			cc.output(string(reply.Err), cc.fromStr(reply.FromReply))
		}

	case OpDelete:
		key := args[0]
		reply := cc.api.Delete(key)
		cc.output(string(reply.Err), cc.fromStr(reply.FromReply))

	case OpPut:
		key, val := args[0], args[1]
		reply := cc.api.Put(key, []byte(val))
		cc.output(string(reply.Err), cc.fromStr(reply.FromReply))

	case OpAppend:
		key, val := args[0], args[1]
		reply := cc.api.Append(key, []byte(val))
		cc.output(string(reply.Err), cc.fromStr(reply.FromReply))

	case OpJoin:
		gid, err := strconv.ParseInt(args[0], 10, 64)
		if err != nil {
			cc.output(fmt.Sprintf("argument [gid] parse error: %v", err))
			return
		}
		nodes := make([]int, 0)
		for _, s := range args[1:] {
			nodeId, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				cc.output(fmt.Sprintf("argument nodeId parse error: %v", err))
				return
			}
			nodes = append(nodes, int(nodeId))
		}

		e := cc.api.Join(int(gid), nodes)
		cc.output(string(e))

	case OpLeave:
		gid, err := strconv.ParseInt(args[0], 10, 64)
		if err != nil {
			cc.output(fmt.Sprintf("argument [gid] parse error: %v", err))
			return
		}
		e := cc.api.Leave(int(gid))
		cc.output(string(e))

	case OpShow:

		parseIds := func() []int {
			ids := make([]int, 0)
			if len(args) > 1 {
				for _, s := range args[1:] {
					id, err := strconv.ParseInt(s, 10, 64)
					if err != nil {
						cc.output(fmt.Sprintf("argument [id] parse error: %v", err))
						return nil
					}
					ids = append(ids, int(id))
				}
			}
			return ids
		}

		if args[0] == "node" {
			ids := parseIds()
			if ids == nil {
				break
			}
			res, e := cc.api.ShowNodes(ids)
			if e!= common.OK {
				cc.output(string(e))
				break
			}
			cc.printShowNodeRes(res)
			cc.output()

		} else if args[0] == "group" {
			ids := parseIds()
			if ids == nil {
				break
			}
			res, e := cc.api.ShowGroups(ids)
			if e != common.OK {
				cc.output(string(e))
				break
			}
			cc.printShowGroupRes(res)
			cc.output()

		} else if args[0] == "shard" {
			ids := parseIds()
			if ids == nil {
				break
			}
			if len(ids) == 0 {
				cc.output("show node operation need at least one gid")
				break
			}
			res, e := cc.api.ShowShards(ids)
			if e != common.OK {
				cc.output(string(e))
				break
			}
			cc.printShowShardRes(res)
			cc.output()

		} else if args[0] == "master" {
			masters := cc.api.ShowMaster()
			cc.printShowMasterRes(masters)
			cc.output()

		} else {
			cc.output("unsupported show information of \"%s\"", args[0])
		}

	case OpTransfer:
		gid, err := strconv.ParseInt(args[0], 10, 64)
		if err != nil {
			cc.output(fmt.Sprintf("argument [gid] parse error: %v", err))
			return
		}
		target, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			cc.output(fmt.Sprintf("argument [gid] parse error: %v", err))
			return
		}
		e := cc.api.TransferLeader(int(gid), int(target))
		cc.output(string(e))
	}
}

func (cc *ConsoleClient) output(lines ...string)  {
	for _, line := range lines {
		_, _ = cc.stdout.WriteString(line)
		_, _ = cc.stdout.WriteString("\n")
	}
	if len(lines) == 0 {
		_, _ = cc.stdout.WriteString("\n")
	}
	_, _ = cc.stdout.WriteString(cc.slash())
	_ = cc.stdout.Flush()
}

func (cc *ConsoleClient) parseInput(line string) (op Operation, args []string, err error)  {
	line = strings.TrimSpace(line)
	line = strings.ToLower(line)
	if line == "" {
		return NoOp, nil, nil
	}
	ss := strings.Split(line, " ")

	opStr := ss[0]
	var ok bool
	if _, ok = opMap[opStr]; !ok {
		err = fmt.Errorf("unsupport operation: %s", opStr)
	}
	op = Operation(opStr)
	if len(ss) > 1 {
		args = ss[1:]
	}
	return
}

func (cc *ConsoleClient) slash() string {
	return fmt.Sprintf("> ")
}

func (cc *ConsoleClient) fromStr(from replica.FromReply) string {
	return fmt.Sprintf("(From node %d, group %d, peer %d)", from.NodeId, from.GID, from.Peer)
}

func printUserGuide(stdout *bufio.Writer) {
	cols := []string{"cmd", "usage", "describe"}

	table, err := gotable.Create(cols...)
	if err != nil {
		panic(err)
	}
	for _, col := range cols {
		table.Align(col, cell.AlignLeft)
	}
	table.CloseBorder()

	writeGuide := func(op Operation, table *table2.Table) {
		opDesc := opMap[string(op)]
		if err := table.AddRow([]string{string(op), opDesc.usage, opDesc.desc}); err != nil {
			panic(err)
		}
	}
	_, _ = stdout.WriteString("----------MultiRaft KV USER GUIDE----------\n")

	cmds := []Operation{OpHelp, OpQuit, OpGet, OpPut, OpAppend, OpDelete, OpJoin, OpLeave, OpShow, OpTransfer}
	for _, cmd := range cmds {
		writeGuide(cmd, table)
	}
	_, _ = stdout.WriteString(table.String())
	// _, _ = stdout.WriteString("\n")

	_ = stdout.Flush()
}

func (cc *ConsoleClient) printShowNodeRes(nodes []master.ShowNodeRes)  {
	stdout := cc.stdout

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Id < nodes[j].Id
	})

	table, err := gotable.Create("NodeId", "Addr", "Groups", "Status")
	if err != nil {
		panic(err)
	}
	for _, node := range nodes {
		if !node.Found {
			continue
		}
		sort.Ints(node.Groups)
		var builder strings.Builder
		for i, gid := range node.Groups {
			if i > 0 {
				builder.WriteString(" ")
			}
			if node.IsLeader[gid] {
				builder.WriteString(fmt.Sprintf("%d(L)", gid))
			} else {
				builder.WriteString(fmt.Sprintf("%d(F)", gid))
			}
		}

		row := []string{strconv.Itoa(node.Id), node.Addr, builder.String(), node.Status}

		if err := table.AddRow(row); err != nil {
			panic(err)
		}
	}
	_, _ = stdout.WriteString(table.String())
}

func (cc *ConsoleClient) printShowGroupRes(groups []master.ShowGroupRes)  {
	stdout := cc.stdout

	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Id < groups[j].Id
	})

	for _, group := range groups {
		if !group.Found {
			continue
		}
		_, _ = stdout.WriteString("\n")
		_, _ = stdout.WriteString(fmt.Sprintf("Group: %d \t\t ShardCount: %d\n", group.Id, group.ShardCnt))
		_, _ = stdout.WriteString(fmt.Sprintf("Group distribute in %d nodes:\n", len(group.ByNode)))

		table, err := gotable.Create("NodeId", "Addr", "Peer", "IsLeader", "ConfNum", "GroupStatus", "Size(bytes)")
		if err != nil {
			panic(err)
		}
		sort.Slice(group.ByNode, func(i, j int) bool {
			return group.ByNode[i].Id < group.ByNode[j].Id
		})
		for _, node := range group.ByNode {
			row := []string{strconv.Itoa(node.Id), node.Addr, strconv.Itoa(node.Peer), strconv.FormatBool(node.IsLeader),
				strconv.Itoa(node.ConfNum), node.Status, strconv.FormatInt(node.Size, 10)}

			if err := table.AddRow(row); err != nil {
				panic(err)
			}
		}
		_, _ = stdout.WriteString(table.String())
	}

}

func (cc *ConsoleClient) printShowShardRes(shards []master.ShowShardRes)  {
	stdout := cc.stdout

	sort.Slice(shards, func(i, j int) bool {
		if shards[i].Gid == shards[j].Gid {
			return shards[i].Id < shards[j].Id
		} else {
			return shards[i].Gid < shards[j].Gid
		}
	})

	table, err := gotable.Create("Id", "Group", "Status", "Size", "Capacity", "RangeStart", "RangeEnd")
	if err != nil {
		panic(err)
	}
	for _, s := range shards {
		// if !s.Found {
		// 	continue
		// }

		row := []string{strconv.Itoa(s.Id), strconv.Itoa(s.Gid) , s.Status.String(), strconv.FormatInt(s.Size, 10),
			strconv.FormatUint(s.Capacity, 10), s.RangeStart, s.RangeEnd}

		if err := table.AddRow(row); err != nil {
			panic(err)
		}
	}
	_, _ = stdout.WriteString(table.String())
}

func (cc *ConsoleClient) printShowMasterRes(masters []master.ShowMasterReply)  {
	stdout := cc.stdout

	table, err := gotable.Create("Id", "Addr", "IsLeader", "ConfNum", "Size", "Status")
	if err != nil {
		panic(err)
	}
	for _, m := range masters {
		// if !s.Found {
		// 	continue
		// }

		row := []string{strconv.Itoa(m.Id), m.Addr , strconv.FormatBool(m.IsLeader), strconv.Itoa(m.LatestConfNum),
			strconv.FormatInt(m.Size, 10), m.Status}

		if err := table.AddRow(row); err != nil {
			panic(err)
		}
	}
	_, _ = stdout.WriteString(table.String())
}