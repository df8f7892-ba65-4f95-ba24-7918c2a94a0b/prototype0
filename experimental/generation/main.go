package main

import (
	"fmt"

	"github.com/df8f7892-ba65-4f95-ba24-7918c2a94a0b/prototype0/crdt"
	"github.com/df8f7892-ba65-4f95-ba24-7918c2a94a0b/prototype0/scalar"
)

type (
	Post struct {
		*crdt.ORSetMap
	}
)

func NewPost() *Post {
	return &Post{
		crdt.NewORSetMap(),
	}
}

func (p *Post) SetTitle(title string) {
	p.Add("title", scalar.New(title))
}

func (p *Post) GetTitle() string {
	return assume[string](p.Get("title"))
}

func (p *Post) SetBody(body string) {
	p.Add("body", scalar.New(body))
}

func (p *Post) GetBody() string {
	return assume[string](p.Get("body"))
}

func main() {
	post := NewPost()
	post.SetTitle("Hello, World!")

	fmt.Println(post.GetTitle())
	fmt.Println(post.GetBody())
}

func assume[T scalar.ScalarValue](v crdt.Value) T {
	s, ok := v.(*scalar.Scalar[T])
	if !ok {
		return *new(T)
	}

	return s.Value
}
