package fs

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"strconv"
	"strings"
)

type BlockDevice interface {
	// ReadBlock reads a block of data (4096 bytes) from the device.
	ReadBlock(blockNum uint64, buf []byte) error
	// WriteBlock writes a block of data (4096 bytes) to the device.
	WriteBlock(blockNum uint64, buf []byte) error
	// Dump prints the contents of the device to stdout.
	Dump()
}

const (
	SuperblockIndex  = 0
	InodeBitmapIndex = 1
	DataBitmapIndex  = 2
	InodeStartIndex  = 3
	// assuming each inode is at most 512 bytes, each block fits
	// 8 inodes. Since we can have at most 32 inodes, this means
	// that our inode table needs to be 32/8 = 4 blocks long.
	DataStartIndex = 3 + 3

	BlockSize = 4096 // bytes
	InodeSize = 512  // bytes
)

type InodeType int

const (
	// InodeTypeFile is a regular file.
	InodeTypeFile = iota
	// InodeTypeDirectory is a directory.
	InodeTypeDirectory
)

type Inode struct {
	Size     uint32 // in bytes
	Index    uint32 // index in the inode list and bitmap
	Type     InodeType
	Blocks   [16]uint32 // block numbers
	Filename string     // up to 128 bytes
	// ...
}

type FileSystem struct {
	dev    BlockDevice
	inodes [32]*Inode
	// For simplicity, we'll just use a byte array to represent the bitmaps.
	// Each byte is either 0 or 1
	inodeBitmap [32]byte // up to 32 inodes
	dataBitmap  [32]byte // up to 32 blocks
}

func NewFileSystem(dev BlockDevice) (*FileSystem, error) {
	// Write the superblock
	superblock := map[string]interface{}{
		"magic": 0xbafdb0,
	}

	// create a 4096 byte buffer containing the superblock
	buf := []byte{}
	// write the magic number to the buffer
	for i := 0; i < 3; i++ {
		buf = append(buf, byte(superblock["magic"].(int)>>uint(8*i)))
	}
	// write the superblock to the device
	err := dev.WriteBlock(SuperblockIndex, buf)
	if err != nil {
		return nil, fmt.Errorf("error writing superblock: %w", err)
	}
	// write the inode bitmap (which is a 1)
	buf = []byte{1}
	err = dev.WriteBlock(1, buf)
	if err != nil {
		return nil, fmt.Errorf("error writing inode bitmap: %w", err)
	}
	// write the data bitmap (which is a 1)
	buf = []byte{1}
	dev.WriteBlock(2, buf)

	rootInode := &Inode{
		Size:     0,
		Index:    0,
		Type:     InodeTypeDirectory,
		Blocks:   [16]uint32{0},
		Filename: "/",
	}

	// write the root inode
	bb := bytes.NewBuffer([]byte{})
	enc := gob.NewEncoder(bb)
	err = enc.Encode(rootInode)
	if err != nil {
		return nil, fmt.Errorf("error encoding root inode: %w", err)
	}
	buf = bb.Bytes()
	dev.WriteBlock(3, buf)

	return &FileSystem{
		dev:         dev,
		inodes:      [32]*Inode{rootInode},
		inodeBitmap: [32]byte{1},
		dataBitmap:  [32]byte{1},
	}, nil
}

func (fs *FileSystem) DisplayInfo() {
	// print inode bitmap
	// print it as a 16x2 bitmap
	fmt.Println("-- inode bitmap --")
	for i := 0; i < 2; i++ {
		for j := 0; j < 16; j++ {
			if fs.inodeBitmap[i*16+j] != 0 {
				fmt.Print("1")
			} else {
				fmt.Print("0")
			}
		}
		fmt.Println()
	}
	fmt.Println()
	// convert inode bitmap into a list of existing inode indices
	inodeIndices := []int{}
	for i := 0; i < 32; i++ {
		if fs.inodeBitmap[i] == 1 {
			inodeIndices = append(inodeIndices, i)
		}
	}
	// print data bitmap
	// print it as a 16x2 bitmap
	fmt.Println("-- data bitmap --")
	for i := 0; i < 2; i++ {
		for j := 0; j < 16; j++ {
			if fs.dataBitmap[i*16+j] != 0 {
				fmt.Print("1")
			} else {
				fmt.Print("0")
			}
		}
		fmt.Println()
	}

	// go through inode indices and decode/print the inodes
	for _, inodeIndex := range inodeIndices {
		inode := fs.inodes[inodeIndex]
		switch inode.Type {
		case InodeTypeFile:
			fmt.Printf("-- file inode %d --\n", inodeIndex)
		case InodeTypeDirectory:
			fmt.Printf("-- directory inode %d --\n", inodeIndex)
		}

		contents, err := fs.ReadInodeContents(uint32(inodeIndex))

		fmt.Printf("size: %d\n", inode.Size)
		fmt.Printf("blocks: %v\n", inode.Blocks)
		fmt.Printf("filename: %s\n", inode.Filename)
		fmt.Printf("contents: %s\n", contents)

		if err != nil {
			fmt.Printf("error reading inode contents: %v\n", err)
		}

		fmt.Println()
	}

	// // dump the contents of the block device
	// fmt.Println("-- block device --")
	// fs.dev.Dump()
}

func LoadFilesystem(dev BlockDevice) (*FileSystem, error) {
	// read the superblock
	buf := make([]byte, BlockSize)
	dev.ReadBlock(SuperblockIndex, buf)
	// read the magic number from the buffer
	magic := 0
	for i := 0; i < 3; i++ {
		magic += int(buf[i]) << uint(8*i)
	}
	// check the magic number
	if magic != 0xbafdb0 {
		return nil, fmt.Errorf("Not a valid filesystem")
	}
	// read the inode bitmap
	dev.ReadBlock(InodeBitmapIndex, buf)
	rawInodeBitmap := buf

	var inodeBitmap [32]byte

	copy(inodeBitmap[:], rawInodeBitmap)

	// convert inode bitmap into a list of existing inode indices
	inodeIndices := []int{}
	for i := 0; i < 32; i++ {
		if inodeBitmap[i] == 1 {
			inodeIndices = append(inodeIndices, i)
		}
	}
	// read the data bitmap
	dev.ReadBlock(DataBitmapIndex, buf)
	rawDataBitmap := buf

	var dataBitmap [32]byte

	copy(dataBitmap[:], rawDataBitmap)

	// go through inode indices and decode/print the inodes
	inodes := [32]*Inode{}
	for i, inodeIndex := range inodeIndices {
		blockIndex := inodeIndex * InodeSize / BlockSize
		blockOffset := inodeIndex * InodeSize % BlockSize
		fmt.Printf("inode %d is in block %d at offset %d\n", inodeIndex, blockIndex+3, blockOffset)
		dev.ReadBlock(uint64(blockIndex+3), buf)
		inodeBytes := buf[blockOffset : blockOffset+InodeSize]
		dec := gob.NewDecoder(bytes.NewBuffer(inodeBytes))
		var inode Inode
		err := dec.Decode(&inode)
		if err != nil {
			return nil, fmt.Errorf("error decoding inode %d: %w\n", inodeIndex, err)
		}
		inodes[i] = &inode
	}

	return &FileSystem{
		dev:         dev,
		inodes:      inodes,
		inodeBitmap: inodeBitmap,
		dataBitmap:  dataBitmap,
	}, nil
}

func (fs *FileSystem) GetInode(inodeIndex uint32) (*Inode, error) {
    if inodeIndex >= 32 { // TODO remove hardcoded size
        return nil, fmt.Errorf("inode index out of bounds: %d", inodeIndex)
    }
    return fs.inodes[inodeIndex], nil
}

func (fs *FileSystem) ReadInodeContents(inodeIndex uint32) (*bytes.Buffer, error) {
	inode := fs.inodes[inodeIndex]

	// read the blocks
	buf := make([]byte, BlockSize)
	bb := bytes.NewBuffer([]byte{})
	for _, blockIndex := range inode.Blocks {
		if blockIndex == 0 {
			break
		}
		fs.dev.ReadBlock(uint64(blockIndex), buf)
		bb.Write(buf)
	}

	return bb, nil
}

func (fs *FileSystem) ReadFileContents(inodeIndex uint32) (*bytes.Buffer, error) {
	inode := fs.inodes[inodeIndex]
	if inode.Type != InodeTypeFile {
		return nil, fmt.Errorf("inode %d is not a file", inodeIndex)
	}

	return fs.ReadInodeContents(inodeIndex)
}

func (fs *FileSystem) ReadDir(inodeIndex uint32) ([]*Inode, error) {
	// The directory is a list of node indices along with their filenames.
	// Example
	// 1 foo
	// 2 bar
	// These are then returned as a list of Inodes

	contents, err := fs.ReadInodeContents(inodeIndex)
	if err != nil {
		return nil, err
	}

	// read the contents
	inodes := []*Inode{}
	scanner := bufio.NewScanner(contents)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, " ")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid line in directory: %s", line)
		}
		inodeIndex, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid inode index in directory: %s", parts[0])
		}
		inode := fs.inodes[inodeIndex]
		inode.Filename = parts[1]
		inodes = append(inodes, inode)
	}

	return inodes, nil
}

func (fs *FileSystem) AddFileToDir(dirInodeIndex uint32, fileInodeIndex uint32) error {
	// read the directory contents
	contents, err := fs.ReadInodeContents(dirInodeIndex)
	if err != nil {
		return err
	}

	// append the new file
	contents.WriteString(fmt.Sprintf("%d %s\n", fileInodeIndex, fs.inodes[fileInodeIndex].Filename))
	// update the size
	fs.inodes[dirInodeIndex].Size = uint32(contents.Len())

	// write the new contents
	err = fs.WriteInodeContents(dirInodeIndex, contents)
	if err != nil {
		return err
	}

	// flush the inode table
	err = fs.WriteInodeTable()

	return nil
}

func (fs *FileSystem) WriteInodeContents(inodeIndex uint32, contents *bytes.Buffer) error {
	nBlocks := (contents.Len() + BlockSize - 1) / BlockSize
	inode := fs.inodes[inodeIndex]
	// write the data blocks
	blocks := make([]byte, nBlocks*BlockSize)
	// copy the contents into the blocks
	copy(blocks, contents.Bytes())

	for i := 0; i < nBlocks; i++ {
		blockIndex := inode.Blocks[i]
		fs.dev.WriteBlock(uint64(blockIndex), blocks[i*BlockSize:(i+1)*BlockSize])
	}

	return nil
}

func (fs *FileSystem) WriteInodeTable() error {
	// write the inode table
	for i := 0; i < len(fs.inodes); i += BlockSize / InodeSize {
		// each block is capable of holding 8 inodes
		// this means that we have to encode 8 inodes at a time
		// then write the block
		buf := make([]byte, BlockSize)
		for j := 0; j < BlockSize/InodeSize; j++ {
			inodeIndex := i + j
			if inodeIndex >= len(fs.inodes) {
				break
			}
			inode := fs.inodes[inodeIndex]
			if inode == nil {
				// write all 0s
				continue
			}
			enc := gob.NewEncoder(bytes.NewBuffer(buf[j*InodeSize : (j+1)*InodeSize]))
			err := enc.Encode(inode)
			if err != nil {
				return fmt.Errorf("error encoding inode %d: %w", inodeIndex, err)
			}
		}
		fs.dev.WriteBlock(uint64(i/8)+InodeStartIndex, buf)
	}

	return nil
}

func (fs *FileSystem) CreateFile(filename string, contents bytes.Buffer) (*Inode, error) {
	path := strings.Split(filename, "/")
	if path[0] != "" {
		return nil, fmt.Errorf("filename must be absolute")
	}
	// find the parent directory
	// start at the root inode
	var parentInodeIndex uint32 = 0
	parentInode := fs.inodes[parentInodeIndex]
	for i := 1; i < len(path)-1; i++ {
		children, err := fs.ReadDir(parentInodeIndex)
		if err != nil {
			return nil, fmt.Errorf("error reading directory %s: %w", path[i], err)
		}
		found := false
		for _, child := range children {
			if child.Filename == path[i] {
				parentInodeIndex = child.Index
				parentInode = child
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("directory %s not found", path[i])
		}
	}

	// check if the parent inode is a directory
	if parentInode.Type != InodeTypeDirectory {
		return nil, fmt.Errorf("parent inode is not a directory")
	}

	// find an empty inode
	inodeIndex := -1
	for i := 0; i < 32; i++ {
		if fs.inodeBitmap[i] == 0 {
			inodeIndex = i
			break
		}
	}

	if inodeIndex == -1 {
		return nil, fmt.Errorf("no empty inodes")
	}

	nBlocks := (contents.Len() + BlockSize - 1) / BlockSize

	dataBlockIndices := []uint32{}

	// find nBlocks empty data blocks
	for i := 0; i < 32; i++ {
		if fs.dataBitmap[i] == 0 {
			dataBlockIndices = append(dataBlockIndices, uint32(i)+DataStartIndex)
			if len(dataBlockIndices) == nBlocks {
				break
			}
		}
	}

	if len(dataBlockIndices) != nBlocks {
		return nil, fmt.Errorf("not enough empty data blocks")
	}

	dataBlockIndicesArray := [16]uint32{}
	copy(dataBlockIndicesArray[:], dataBlockIndices)

	// create the inode
	inode := &Inode{
		Index:    uint32(inodeIndex),
		Type:     InodeTypeFile,
		Size:     uint32(contents.Len()),
		Blocks:   dataBlockIndicesArray,
		Filename: path[len(path)-1],
	}

	// write the inode to the inode table
	fs.inodes[inodeIndex] = inode
	err := fs.WriteInodeTable()
	if err != nil {
		return nil, fmt.Errorf("error writing inode table: %w", err)
	}

	// write inode contents
	err = fs.WriteInodeContents(inode.Index, &contents)
	if err != nil {
		return nil, fmt.Errorf("error writing inode contents: %w", err)
	}

	// update the parent directory
	err = fs.AddFileToDir(parentInodeIndex, uint32(inodeIndex))
	if err != nil {
		return nil, fmt.Errorf("error adding file to directory: %w", err)
	}

	// update the inode bitmap
	fs.inodeBitmap[inodeIndex] = 1
	// write the inode bitmap
	fs.dev.WriteBlock(InodeBitmapIndex, fs.inodeBitmap[:])

	// update the data bitmap
	for _, blockIndex := range dataBlockIndices {
		fs.dataBitmap[blockIndex] = 1
	}
	// write the data bitmap
	fs.dev.WriteBlock(DataBitmapIndex, fs.dataBitmap[:])

	return inode, nil
}

type ArrayBlockDevice struct {
	buf []byte
}

func NewArrayBlockDevice(buf []byte) *ArrayBlockDevice {
	return &ArrayBlockDevice{buf}
}

// ReadBlock reads a block from the device into the buffer
func (dev *ArrayBlockDevice) ReadBlock(blockNum uint64, buf []byte) error {
	copy(buf, dev.buf[blockNum*4096:(blockNum+1)*4096])
	return nil
}

// WriteBlock writes a block from the buffer to the device
func (dev *ArrayBlockDevice) WriteBlock(blockNum uint64, buf []byte) error {
	copy(dev.buf[blockNum*4096:(blockNum+1)*4096], buf)
	return nil
}

// Dump prints the contents of the device
func (dev *ArrayBlockDevice) Dump() {
	fmt.Printf("ArrayBlockDevice: %d bytes\n", len(dev.buf))
	for i := 0; i < len(dev.buf); i++ {
		fmt.Printf("%02x ", dev.buf[i])
		if i%16 == 15 {
			fmt.Println()
		}
	}
	fmt.Println()
}

