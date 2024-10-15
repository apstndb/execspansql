package main

import (
	"github.com/samber/lo"
	"iter"
	"spheric.cloud/xiter"
	"strings"
)

func Must2[T1, T2, R any](f func(T1, T2) (R, error)) func(T1, T2) R {
	return func(v1 T1, v2 T2) R {
		return lo.Must(f(v1, v2))
	}
}

func Uncurry[T1, T2, R any](f func(T1, T2) R) func(xiter.Zipped[T1, T2]) R {
	return func(z xiter.Zipped[T1, T2]) R {
		return f(z.V1, z.V2)
	}
}
func UncurryErr[T1, T2, R any](f func(T1, T2) (R, error)) func(xiter.Zipped[T1, T2]) (R, error) {
	return func(z xiter.Zipped[T1, T2]) (R, error) {
		return f(z.V1, z.V2)
	}
}

func tryJoin(delegate iter.Seq2[string, error], sep string) (string, error) {
	first := true
	var b strings.Builder
	for s, err := range delegate {
		if err != nil {
			return "", err
		}
		if !first {
			b.WriteString(sep)
		} else {
			first = false
		}
		b.WriteString(s)
	}
	return b.String(), nil
}
