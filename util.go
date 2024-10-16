package main

import (
	"github.com/samber/lo"
	"iter"
	"spheric.cloud/xiter"
	"strings"
)

func must2[T1, T2, R any](f func(T1, T2) (R, error)) func(T1, T2) R {
	return func(v1 T1, v2 T2) R {
		return lo.Must(f(v1, v2))
	}
}

func tupled[T1, T2, R any](f func(T1, T2) R) func(xiter.Zipped[T1, T2]) R {
	return func(z xiter.Zipped[T1, T2]) R {
		return f(z.V1, z.V2)
	}
}

func tupledWithErr[T1, T2, R any](f func(T1, T2) (R, error)) func(xiter.Zipped[T1, T2]) (R, error) {
	return func(z xiter.Zipped[T1, T2]) (R, error) {
		return f(z.V1, z.V2)
	}
}

func tryJoin(delegate iter.Seq2[string, error], sep string) (string, error) {
	first := true
	var b strings.Builder
	for s, err := range delegate {
		if err != nil {
			return b.String(), err
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

func tryMapMap[K comparable, T, R any](m map[K]T, f func(K, T) (R, error)) (map[K]R, error) {
	res := make(map[K]R, len(m))
	for k, v := range m {
		newV, err := f(k, v)
		if err != nil {
			return res, err
		}
		res[k] = newV
	}
	return res, nil
}

// mapNonError apply function to the first value of pair in iter.Seq2.
// It passes through values if error isn't nil, and the first value become zero value of the return type of f.
func mapNonError[T, R any](delegate iter.Seq2[T, error], f func(T) R) iter.Seq2[R, error] {
	return func(yield func(R, error) bool) {
		for v, err := range delegate {
			if err != nil {
				ok := yield(lo.Empty[R](), err)
				if !ok {
					return
				}
				continue
			}
			ok := yield(f(v), err)
			if !ok {
				return
			}
		}
	}
}
