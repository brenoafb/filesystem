package fs

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFSInit(t *testing.T) {
	// create a 32KiB array
	disk := make([]byte, 32*1024)
	// create a BlockDevice that uses the array as storage
	dev := NewArrayBlockDevice(disk)

	// create a filesystem on the device
	filesystem, err := NewFileSystem(dev)

	require.NoError(t, err)

	// Test that superblock was properly written
	buf := make([]byte, BlockSize)
	err = dev.ReadBlock(SuperblockIndex, buf)

	require.NoError(t, err)

	require.Equal(t, byte(0xb0), byte(buf[0]))
	require.Equal(t, byte(0xfd), byte(buf[1]))
	require.Equal(t, byte(0xba), byte(buf[2]))
	require.Equal(t, byte(0), byte(buf[3]))

	// Test that the initial inode bitmap was properly written
	buf = make([]byte, BlockSize)
	err = dev.ReadBlock(InodeBitmapIndex, buf)

	require.NoError(t, err)

	// we start with one inode taken
	require.Equal(t, byte(1), byte(buf[0]))
	// all the following inodes are free
	for i := 1; i < BlockSize; i++ {
		require.Equal(t, byte(0), byte(buf[i]))
	}

	// Test that the initial data bitmap was properly written
	buf = make([]byte, BlockSize)
	err = dev.ReadBlock(DataBitmapIndex, buf)

	require.NoError(t, err)

	// All data blocks are free
	for i := 0; i < BlockSize; i++ {
		require.Equal(t, byte(0), byte(buf[i]))
	}

	// Check that the root file was properly written
	inode, err := filesystem.GetInode(0)
	require.NoError(t, err)
	require.Equal(t, uint32(0), inode.Size)
	require.Equal(t, uint32(0), inode.Index)
	require.Equal(t, InodeTypeDirectory, inode.Type)
	require.Equal(t, "/", inode.Filename)

	_, err = filesystem.ReadInodeContents(0)
	require.NoError(t, err)

	dir, err := filesystem.ReadDir(0)
	require.NoError(t, err)
	require.Equal(t, 0, len(dir))
}

func TestCreateFile(t *testing.T) {
	// create a 32KiB array
	disk := make([]byte, 32*1024)
	// create a BlockDevice that uses the array as storage
	dev := NewArrayBlockDevice(disk)

	// create a filesystem on the device
	filesystem, err := NewFileSystem(dev)

	require.NoError(t, err)

	// Create a file
	str := "hello world"
	contents := bytes.NewBuffer([]byte(str))
	inode, err := filesystem.CreateFile("/foo", contents)
	require.NoError(t, err)

	// Check that the file was properly written
	require.Equal(t, inode.Filename, "foo")
	require.Equal(t, inode.Type, InodeType(InodeTypeFile))
	require.Equal(t, inode.Size, uint32(len(str)))

	_, err = filesystem.ReadInodeContents(1)
	require.NoError(t, err)

	// check that the root directory got updated
	dir, err := filesystem.ReadDir(0)
	require.NoError(t, err)
	require.Equal(t, len(dir), 1)

	require.Equal(t, dir[0].Filename, "foo")
	require.Equal(t, dir[0].Type, InodeType(InodeTypeFile))
	require.Equal(t, dir[0].Size, uint32(len(str)))
}
