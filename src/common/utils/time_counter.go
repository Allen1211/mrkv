package utils

import "time"

type TimeCounter struct {
	begin	time.Time
}

func (c *TimeCounter) Reset() {
	c.begin = time.Now()
}

func (c *TimeCounter) Count() int64 {
	return time.Now().Sub(c.begin).Microseconds()
}

func (c *TimeCounter) CountSecond() float64 {
	return time.Now().Sub(c.begin).Seconds()
}

func (c *TimeCounter) CountMilliseconds() int64 {
	return time.Now().Sub(c.begin).Milliseconds()
}

func (c *TimeCounter) CountMicroseconds() int64 {
	return time.Now().Sub(c.begin).Microseconds()
}

func (c *TimeCounter) CountNanoseconds() int64 {
	return time.Now().Sub(c.begin).Nanoseconds()
}