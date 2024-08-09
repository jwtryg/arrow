package testing

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/apache/arrow/go/v18/arrow/avro"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/compress"
)

func arrow_test() {
	// Create a new UserBuffer
	buffer := NewUserBuffer(10)
	defer buffer.Close()

	// Put some users into the buffer
	buffer.Put(User{Name: "Alice", Age: 30})
	buffer.Put(User{Name: "Bob", Age: 25})
	buffer.Put(User{Name: "Charlie", Age: 35})
	buffer.Put(User{Name: "David", Age: 40})
	buffer.Put(User{Name: "Eve", Age: 20})

	// Time the program
	ts := time.Now()

	// Run the program
	log.Println("starting:")
	av2arReader, err := avro.NewAvroReader(UserSchema.String(), buffer)
	if err != nil {
		fmt.Println(err)
		os.Exit(3)
	}
	defer av2arReader.Close()

	fp, err := os.OpenFile(*filepath+".parquet", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		fmt.Println(err)
		os.Exit(4)
	}
	defer fp.Close()
	pwProperties := parquet.NewWriterProperties(parquet.WithDictionaryDefault(true),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithBatchSize(1024*32),
		parquet.WithDataPageSize(1024*1024),
		parquet.WithMaxRowGroupLength(64*1024*1024),
	)
	awProperties := pq.NewArrowWriterProperties(pq.WithStoreSchema())
	pr, err := pq.NewFileWriter(av2arReader.Schema(), fp, pwProperties, awProperties)
	if err != nil {
		fmt.Println(err)
		os.Exit(5)
	}
	defer pr.Close()
	fmt.Printf("parquet version: %v\n", pwProperties.Version())
	for av2arReader.Next() {
		if av2arReader.Err() != nil {
			fmt.Println(err)
			os.Exit(6)
		}
		recs := av2arReader.Record()
		err = pr.WriteBuffered(recs)
		if err != nil {
			fmt.Println(err)
			os.Exit(7)
		}
		recs.Release()
	}
	if av2arReader.Err() != nil {
		fmt.Println(av2arReader.Err())
	}

	pr.Close()
	log.Printf("time to convert: %v\n", time.Since(ts))
}
