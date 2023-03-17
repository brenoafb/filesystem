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

type InodeType uint32

const (
	// InodeTypeFile is a regular file.
	InodeTypeFile InodeType = iota
	// InodeTypeDirectory is a directory.
	InodeTypeDirectory
)

type Inode struct {
	// Size represents the size of the file in number of bytes
	Size uint32
	// Index represents the index of the inode
	Index uint32
	// Type indicates whether it's a regular file or a directory
	Type InodeType
	// Blocks contains the index of the blocks occupied by the file.
	// If the file is smaller than 16 blocks, the remaining block indices
	// are set to 0.
	// Meaning that the blocks occupied by the file are B[0] through B[i],
	// where i is the largest number for which B[i] > 0.
	Blocks [16]uint32 // block numbers
	// Filename contains the file's relative name.
	// It can be up to 128 bytes in size.
	Filename string
	// ...
}

type FileSystem struct {
	// dev is the underlying block device
	dev BlockDevice
	// inode list
	inodes [32]*Inode
	// For simplicity, we'll just use a byte array to represent the bitmaps.
	// Each byte is either 0 or 1
	// indicates which inodes are taken
	inodeBitmap [32]byte // up to 32 inodes
	// indicates which data blocks are taken
	dataBitmap [32]byte // up to 32 blocks
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
	// write the inode bitmap (which is a 1 since we have only the root dir inode)
	buf = []byte{1}
	err = dev.WriteBlock(InodeBitmapIndex, buf)
	if err != nil {
		return nil, fmt.Errorf("error writing inode bitmap: %w", err)
	}
	// write the data bitmap (which is a 0 since no data is allocated yet)
	buf = []byte{0}
	dev.WriteBlock(DataBitmapIndex, buf)

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
	dev.WriteBlock(InodeStartIndex, buf)

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

		contents, err := fs.ReadInodeContents(inodeIndex)

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

func (fs *FileSystem) GetInode(inodeIndex int) (*Inode, error) {
	if inodeIndex >= 32 { // TODO remove hardcoded size
		return nil, fmt.Errorf("inode index out of bounds: %d", inodeIndex)
	}
	return fs.inodes[inodeIndex], nil
}

func (fs *FileSystem) ReadInodeContents(inodeIndex int) (*bytes.Buffer, error) {
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

	// trim to the correct file size
	bb.Truncate(int(inode.Size))

	return bb, nil
}

func (fs *FileSystem) ReadFileContents(inodeIndex int) (*bytes.Buffer, error) {
	inode := fs.inodes[inodeIndex]
	if inode.Type != InodeTypeFile {
		return nil, fmt.Errorf("inode %d is not a file", inodeIndex)
	}

	return fs.ReadInodeContents(inodeIndex)
}

func (fs *FileSystem) ReadDir(inodeIndex int) ([]*Inode, error) {
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

func (fs *FileSystem) AddFileToDir(dirInodeIndex int, fileInodeIndex int) error {
	// read the directory contents
	inode := fs.inodes[dirInodeIndex]
	contents, err := fs.ReadInodeContents(dirInodeIndex)
	if err != nil {
		return err
	}

	// append the new file
	contents.WriteString(fmt.Sprintf("%d %s\n", fileInodeIndex, fs.inodes[fileInodeIndex].Filename))
	// update the size
	fs.inodes[dirInodeIndex].Size = uint32(contents.Len())
	// check if the current number of blocks allocated to the file suffice
	nCurrentBlocks := 0
	blockEndIndex := 0
	for i, blockIndex := range inode.Blocks {
		// Only nonzero blocks indicate actual blocks used by the file
		// Whenever we reach a 0, it means that there are no more blocks taken
		// by the file
		if blockIndex == 0 {
			blockEndIndex = i
			break
		}
		nCurrentBlocks += 1
	}

	nTotalBlocks := GetSizeInBlocks(contents.Len())

	if nTotalBlocks <= nCurrentBlocks {
		// Current block count is enough
	} else {
		// We need extra blocks to fit the new content
		// find nBlocks empty data blocks
		added := 0
		for i := 0; i < 32; i++ {
			if fs.dataBitmap[i] == 0 {
				// Found an empty block
				// Remember that block indices are absolute,
				// meaning that we have to add the start offset
				inode.Blocks[blockEndIndex+added] = uint32(i) + DataStartIndex
				fs.dataBitmap[i] = 1
				added++
				if added == nTotalBlocks-nCurrentBlocks {
					break
				}
			}
		}

		if added < nTotalBlocks-nCurrentBlocks {
			return fmt.Errorf("not enough free blocks to fit the new directory contents")
		}
	}

	// write the new contents
	err = fs.WriteInodeContents(dirInodeIndex, contents)
	if err != nil {
		return err
	}

	// flush the inode table
	err = fs.WriteInodeTable()

	// write the data bitmap
	fs.PersistDataBitmap()

	return nil
}

func (fs *FileSystem) WriteInodeContents(inodeIndex int, contents *bytes.Buffer) error {
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

func (fs *FileSystem) CreateFile(filename string, contents *bytes.Buffer) (*Inode, error) {
	parentInode, err := fs.FindParentInodeByName(filename)

	if err != nil {
		return nil, fmt.Errorf("error when finding parent inode: %w", err)
	}

	// check if the parent inode is a directory
	if parentInode.Type != InodeTypeDirectory {
		return nil, fmt.Errorf("parent inode is not a directory")
	}

	// find an free inode
	inodeIndex, err := fs.FindFreeInode()

	if err != nil {
		return nil, fmt.Errorf("error when finding free inode: %w", err)
	}

	nBlocks := GetSizeInBlocks(contents.Len())

	dataBlockIndices, err := fs.FindEmptyBlocks(nBlocks)

	if err != nil {
		return nil, fmt.Errorf("error when finding blocks for new file: %w", err)
	}

	dataBlockIndicesArray := [16]uint32{}
	copy(dataBlockIndicesArray[:], dataBlockIndices)

	// create the inode
	inode := &Inode{
		Index:    uint32(inodeIndex),
		Type:     InodeTypeFile,
		Size:     uint32(contents.Len()),
		Blocks:   dataBlockIndicesArray,
		Filename: GetRelativePathFromAbsolute(filename),
	}

	// write the inode to the inode table
	fs.inodes[inodeIndex] = inode
	err = fs.WriteInodeTable()
	if err != nil {
		return nil, fmt.Errorf("error writing inode table: %w", err)
	}

	// write inode contents
	err = fs.WriteInodeContents(int(inode.Index), contents)
	if err != nil {
		return nil, fmt.Errorf("error writing inode contents: %w", err)
	}

	// update the inode bitmap
	fs.inodeBitmap[inodeIndex] = 1

	// write the inode bitmap
	err = fs.PersistInodeBitmap()

	// update the data bitmap
	for _, blockIndex := range dataBlockIndices {
		fs.dataBitmap[blockIndex] = 1
	}
	// write the data bitmap
	err = fs.PersistDataBitmap()
	if err != nil {
		return nil, fmt.Errorf("error persisting inode bitmap when creating file: %w", err)
	}

	if err != nil {
		return nil, fmt.Errorf("error persisting data bitmap when creating file: %w", err)
	}

	// update the parent directory
	err = fs.AddFileToDir(int(parentInode.Index), inodeIndex)
	if err != nil {
		return nil, fmt.Errorf("error adding file to directory: %w", err)
	}

	return inode, nil
}

func (fs *FileSystem) FindInodeByName(filename string) (*Inode, error) {
	path := strings.Split(filename, "/")
	if path[0] != "" {
		return nil, fmt.Errorf("filename must be absolute")
	}
	return fs.traversePath(path)
}

func (fs *FileSystem) FindParentInodeByName(filename string) (*Inode, error) {
	path := strings.Split(filename, "/")
	if path[0] != "" {
		return nil, fmt.Errorf("filename must be absolute")
	}
	return fs.traversePath(path[:len(path)-1])
}

func GetRelativePathFromAbsolute(filename string) string {
	path := strings.Split(filename, "/")
	if path[0] != "" {
		return ""
	}
	return strings.Join(path[1:], "/")
}

func (fs *FileSystem) traversePath(path []string) (*Inode, error) {
	// start at the root inode
	inodeIndex := 0
	inode := fs.inodes[inodeIndex]
	for i := 1; i < len(path); i++ {
		children, err := fs.ReadDir(inodeIndex)
		if err != nil {
			return nil, fmt.Errorf("error reading directory %s: %w", path[i], err)
		}
		found := false
		for _, child := range children {
			if child.Filename == path[i] {
				inodeIndex = int(child.Index)
				inode = child
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("directory %s not found", path[i])
		}
	}

	return inode, nil
}

func (fs *FileSystem) FindFreeInode() (int, error) {
	for i := 0; i < 32; i++ {
		if fs.inodeBitmap[i] == 0 {
			return i, nil
		}
	}

	return 0, fmt.Errorf("no empty inodes")
}

func (fs *FileSystem) PersistDataBitmap() error {
	return fs.dev.WriteBlock(DataBitmapIndex, fs.dataBitmap[:])
}

func (fs *FileSystem) PersistInodeBitmap() error {
	return fs.dev.WriteBlock(InodeBitmapIndex, fs.inodeBitmap[:])
}

func (fs *FileSystem) FindEmptyBlocks(n int) ([]uint32, error) {
	dataBlockIndices := []uint32{}

	for i := 0; i < 32; i++ {
		if fs.dataBitmap[i] == 0 {
			dataBlockIndices = append(dataBlockIndices, uint32(i)+DataStartIndex)
			if len(dataBlockIndices) == n {
				break
			}
		}
	}

	if len(dataBlockIndices) != n {
		return dataBlockIndices, fmt.Errorf("not enough empty data blocks")
	}

	return dataBlockIndices, nil
}

// GetSizeInBlocks computes how many blocks n bytes take up
func GetSizeInBlocks(n int) int {
	return (n + BlockSize - 1) / BlockSize
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
