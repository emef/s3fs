package cmd

import (
	"io"
	"os"

	"github.com/golang/glog"
	"github.com/spf13/cobra"
)

var (
	destPathForRead string
)

func init() {
	readCmd.Flags().StringVarP(&destPathForRead, "file", "f", "",
		"Local file to write outptut to (otherwise output to stdout)")
	rootCmd.AddCommand(readCmd)
}

var readCmd = &cobra.Command{
	Use:   "read <s3_path>",
	Short: "Read some data from s3",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		srcPath := args[0]

		dest := os.Stdout
		if destPathForRead != "" {
			destFile, err := os.OpenFile(destPathForRead, os.O_RDWR|os.O_CREATE, 0755)
			if err != nil {
				glog.Fatalf("Error opening '%v': %v\n", destPathForRead, err)
			}

			dest = destFile
		}

		src := s3fs.Read(srcPath)

		if _, err := io.Copy(dest, src); err != nil {
			glog.Fatalf("Error: %v\n", err)
		}
	},
}
