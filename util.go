package main

import (
	"deedles.dev/xiter"
	"github.com/samber/lo"
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
