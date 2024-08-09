package testing

import (
	"fmt"
	"io"
	"testing"

	"github.com/hamba/avro/v2"
)

var UserSchema avro.Schema = avro.MustParse(`{
	"type": "record",
	"name": "User",
	"fields": [
		{"name": "Name", "type": "string"},
		{"name": "Age", "type": "int"}
	]
}`)

type User struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

type UserBuffer struct {
	ch chan User
}

func NewUserBuffer(bufsize int) *UserBuffer {
	return &UserBuffer{
		ch: make(chan User, bufsize),
	}
}

func (r *UserBuffer) Read(buf []byte) (int, error) {
	user, ok := <-r.ch
	if !ok {
		return 0, io.EOF
	}

	userJson, err := avro.Marshal(UserSchema, user)
	if err != nil {
		return 0, err
	}

	written := len(userJson)
	if written > len(buf) {
		return 0, fmt.Errorf("buffer is too small")
	}
	copy(buf, userJson)

	return written, nil
}

func (r *UserBuffer) Put(user User) {
	r.ch <- user
}

func (r *UserBuffer) Close() {
	close(r.ch)
}

func TestAvro(t *testing.T) {
	// Create a new UserBuffer
	buffer := NewUserBuffer(10)
	defer buffer.Close()

	// Create a new Avro Decoder
	decoder := avro.NewDecoderForSchema(UserSchema, buffer)

	// Create new user record
	buffer.Put(User{
		Name: "Jonas",
		Age:  28,
	})
	buffer.Put(User{
		Name: "Anna",
		Age:  27,
	})

	// Decode the user record
	var u User
	if err := decoder.Decode(&u); err != nil {
		panic(err)
	}
	if err := decoder.Decode(&u); err != nil {
		panic(err)
	}
}
