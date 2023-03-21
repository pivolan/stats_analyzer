package main

import (
	"archive/zip"
	"compress/gzip"
	"fmt"
	"github.com/pierrec/lz4"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func unpackArchive(filePath string) (string, error) {
	ext := filepath.Ext(filePath)
	if ext == ".zip" {
		return unpackZipArchive(filePath)
	} else if ext == ".gz" {
		return unpackGzipArchive(filePath)
	} else if ext == ".lz4" {
		return unpackLZ4Archive(filePath)
	}
	return "", nil
}

func unpackZipArchive(filePath string) (string, error) {
	// Open zip archive
	r, err := zip.OpenReader(filePath)
	if err != nil {
		return "", err
	}
	defer r.Close()

	// Find largest file in archive
	var largestFile *zip.File
	var largestSize uint64
	for _, f := range r.File {
		if f.FileInfo().IsDir() {
			continue
		}
		if f.UncompressedSize64 > largestSize {
			largestFile = f
			largestSize = f.UncompressedSize64
		}
	}
	if largestFile == nil {
		return "", nil
	}

	// Extract largest file to same directory
	destPath := filepath.Join(filepath.Dir(filePath), largestFile.Name)
	os.MkdirAll(filepath.Dir(destPath), 0755)
	outFile, err := os.Create(destPath)
	if err != nil {
		fmt.Println("create1", destPath)
		return "", err
	}
	defer outFile.Close()
	rc, err := largestFile.Open()
	if err != nil {
		fmt.Println("open1")
		return "", err
	}
	defer rc.Close()
	_, err = io.Copy(outFile, rc)
	if err != nil {
		fmt.Println("copy1")
		return "", err
	}

	// Remove original archive
	err = os.Remove(filePath)
	if err != nil {
		return "", err
	}

	return destPath, nil
}

func unpackGzipArchive(filePath string) (string, error) {
	// Open gzip archive
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	gr, err := gzip.NewReader(file)
	if err != nil {
		return "", err
	}
	defer gr.Close()

	// Create output file
	destPath := strings.TrimSuffix(filePath, ".gz")
	outFile, err := os.Create(destPath)
	if err != nil {
		return "", err
	}
	defer outFile.Close()

	// Uncompress gzip to output file
	_, err = io.Copy(outFile, gr)
	if err != nil {
		return "", err
	}

	// Remove original archive
	err = os.Remove(filePath)
	if err != nil {
		return "", err
	}

	return destPath, nil
}

func unpackLZ4Archive(filePath string) (string, error) {
	// Open LZ4 archive
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Create output file
	destPath := strings.TrimSuffix(filePath, ".lz4")
	outFile, err := os.Create(destPath)
	if err != nil {
		return "", err
	}
	defer outFile.Close()

	// Uncompress LZ4 to output file
	_, err = io.Copy(outFile, lz4.NewReader(file))
	if err != nil {
		return "", err
	}

	// Remove original archive
	err = os.Remove(filePath)
	if err != nil {
		return "", err
	}

	return destPath, nil
}
