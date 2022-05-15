package utils

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

func CheckAndMkdir(dir string) error {
	stat, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			if err1 := os.MkdirAll(dir, 0666); err1 != nil {
				return err1
			}
			stat, _ = os.Stat(dir)
		} else {
			return err
		}
	}
	if !stat.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}
	return nil
}

func ReadFile(path string) []byte {
	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		if os.IsNotExist(err) {
			return []byte{}
		} else {
			log.Fatalf("failed to open file: %v\n", err)
		}
	}
	defer file.Close()
	bytes, err := ioutil.ReadAll(file)
	if err != nil || bytes == nil {
		log.Fatalf("failed to read file: %v\n", err)
	}
	return bytes
}

func WriteFile(path string, data []byte) {
	file, err := os.Create(path)
	if err != nil {
		log.Fatalf("failed to create file: %v\n", err)
	}
	defer file.Close()
	if _, err := file.Write(data); err != nil {
		log.Fatalf("failed to write file: %v\n", err)
	}
}

func SizeOfFile(path string) int {
	stat, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		log.Fatalf("failed to stat file: %v\n", err)
	} else if stat.IsDir() {
		log.Fatalf("file path %s is directory, expected a file\n", path)
	}
	return int(stat.Size())
}

func DeleteFile(path string) {
	if err := os.Remove(path); err != nil {
		// fmt.Printf("cannot delete file %s: %v\n", path, err)
	}
}

func DeleteDir(path string) {
	if err := os.RemoveAll(path); err != nil {
		// fmt.Printf("cannot delete dir %s: %v\n", path, err)
	}
}

func SizeOfDir(path string) int64 {
	res := int64(0)
	err := filepath.Walk(path, func(path string, info fs.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			res += info.Size()
		}
		return err
	})
	if err != nil {
		return -1
	}
	return res
}