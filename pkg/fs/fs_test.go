package fs

import (
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

	require.Equal(t, byte(buf[0]), byte(0xb0))
	require.Equal(t, byte(buf[1]), byte(0xfd))
	require.Equal(t, byte(buf[2]), byte(0xba))
	require.Equal(t, byte(buf[3]), byte(0))

	// Test that the initial inode bitmap was properly written
	buf = make([]byte, BlockSize)
	err = dev.ReadBlock(InodeBitmapIndex, buf)

	require.NoError(t, err)

	require.Equal(t, byte(buf[0]), byte(1))
	require.Equal(t, byte(buf[1]), byte(0))

	// Test that the initial data bitmap was properly written
	buf = make([]byte, BlockSize)
	err = dev.ReadBlock(DataBitmapIndex, buf)

	require.NoError(t, err)

	require.Equal(t, byte(buf[0]), byte(1))
	require.Equal(t, byte(buf[1]), byte(0))

	// Check that the root file was properly written
	inode, err := filesystem.GetInode(0)
	require.NoError(t, err)
	require.Equal(t, *inode, Inode{
		Size:     0,
		Index:    0,
		Type:     InodeTypeDirectory,
		Filename: "/",
	})

	_, err = filesystem.ReadInodeContents(0)
	require.NoError(t, err)

	dir, err := filesystem.ReadDir(0)
	require.NoError(t, err)
	require.Equal(t, len(dir), 0)
}
