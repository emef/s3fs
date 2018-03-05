package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	srcPathForWrite string
)

func init() {
	writeCmd.Flags().StringVarP(&srcPathForWrite, "file", "f", "",
		"Source file to be written (otherwise read from stdin)")
	rootCmd.AddCommand(writeCmd)
}

var writeCmd = &cobra.Command{
	Use:   "write <s3_path>",
	Short: "Writes some data to s3",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dest := args[0]

		data := os.Stdin
		if srcPathForWrite != "" {
			fileData, err := os.Open(srcPathForWrite)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error opening '%v': %v\n", srcPathForWrite, err)
				os.Exit(1)
			}

			data = fileData
		}

		if err := s3fs.Write(dest, data); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	},
}
