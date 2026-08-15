package navi

import (
	"fmt"
	"time"
)

func (s *Server) debugmsg(level, msg string) string {
	return fmt.Sprintf("%s [%s] [Id: %d, Term: %d] %s", time.Now().Format(time.RFC3339Nano), level, s.id, s.currentTerm, msg)
}

func (s *Server) debug(msg string) {
	if !s.Debug {
		return
	}
	fmt.Println(s.debugmsg("INFO", msg))
}

func (s *Server) debugf(msg string, args ...any) {
	if !s.Debug {
		return
	}

	s.debug(fmt.Sprintf(msg, args...))
}

func (s *Server) warn(msg string) {
	fmt.Println(s.debugmsg("WARN", msg))
}

func (s *Server) warnf(msg string, args ...any) {
	s.warn(fmt.Sprintf(msg, args...))
}

func (s *Server) error(msg string) {
	fmt.Println(s.debugmsg("ERROR", msg))
}

func (s *Server) errorf(msg string, args ...any) {
	s.error(fmt.Sprintf(msg, args...))
}

func Assert[T comparable](msg string, a, b T) {
	if a != b {
		panic(fmt.Sprintf("%s. Got a = %#v, b = %#v", msg, a, b))
	}
}

func ServerAssert[T comparable](s *Server, msg string, a, b T) {
	Assert(s.debugmsg("ERROR", msg), a, b)
}
