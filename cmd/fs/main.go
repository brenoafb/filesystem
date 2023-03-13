package main

import (
    "bytes"
    "fmt"

    "brenoafb.com/very-simple-filesystem/pkg/fs"
)

func main() {
	// create a 32KiB array
	disk := make([]byte, 32*1024)
	// create a BlockDevice that uses the array as storage
	dev := fs.NewArrayBlockDevice(disk)

	// create a filesystem on the device
	filesystem, err := fs.NewFileSystem(dev)

	if err != nil {
		panic(err)
	}

	// display the filesystem info
	filesystem.DisplayInfo()

	// Add a file
	contentString := "Hello, world!"
	content := bytes.NewBufferString(contentString)
	inode, err := filesystem.CreateFile("/foo.txt", *content)
	if err != nil {
		panic(err)
	}

	// display the filesystem info
	filesystem.DisplayInfo()

	// Read back the file
	buf, err := filesystem.ReadFileContents(inode.Index)

	if err != nil {
		panic(err)
	}

	fmt.Printf("File contents: %s\n", buf.String())
}
