// update_examples.go updates README.md code examples with actual example code.
//
// This program reads Go source files from the examples/ directory and replaces
// the corresponding code blocks in README.md with the actual working code.
// It ensures that documentation stays in sync with real examples and converts
// tabs to spaces for proper markdown formatting.
//
// Usage:
//
//	go run examples/update_examples.go
//
// The program automatically finds and updates all mapped code sections in
// README.md based on the exampleMappings configuration.
package main

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"strings"
)

// ExampleMapping defines the relationship between a README section and its
// corresponding example file.
type ExampleMapping struct {
	// StartMarker is the markdown heading that begins the section to update
	StartMarker string
	// ExamplePath is the relative path to the Go example file
	ExamplePath string
}

// exampleMappings defines which example files correspond to which README sections.
// Each mapping identifies a section in README.md by its start marker and specifies
// which example file should replace the first Go code block found after that marker.
var exampleMappings = []ExampleMapping{
	{
		StartMarker: "### Generic API (Recommended)",
		ExamplePath: "examples/generic/generic.go",
	},
	{
		StartMarker: "### String Sorting",
		ExamplePath: "examples/strings/strings.go",
	},
	{
		StartMarker: "### Custom Types with Generic API",
		ExamplePath: "examples/custom_types/custom_types.go",
	},
	{
		StartMarker: "## Legacy Interface-Based API",
		ExamplePath: "examples/legacy/legacy.go",
	},
	{
		StartMarker: "### Basic String Diff",
		ExamplePath: "examples/diff/diff.go",
	},
	{
		StartMarker: "### Generic Diff for Custom Types",
		ExamplePath: "examples/diff_generic/diff_generic.go",
	},
	{
		StartMarker: "### Parallel Diff Processing",
		ExamplePath: "examples/diff_parallel/diff_parallel.go",
	},
}

// extractGoCode reads a Go source file and extracts the code content without
// the package declaration. It also converts tabs to spaces for markdown formatting.
// Returns the extracted code with proper indentation for markdown code blocks.
func extractGoCode(filePath string) (string, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return "", err
	}

	// Parse the Go file
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, filePath, content, parser.ParseComments)
	if err != nil {
		return "", err
	}

	// Extract the code without the package declaration
	start := fset.Position(node.Name.End()).Offset + 1

	// Skip any blank lines after package declaration
	codeContent := string(content[start:])
	lines := strings.Split(codeContent, "\n")

	var actualStart int
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			actualStart = strings.Index(codeContent, line)
			break
		}
	}

	extractedCode := string(content[start+actualStart:])

	// Convert tabs to spaces (4 spaces per tab for markdown)
	extractedCode = strings.ReplaceAll(extractedCode, "\t", "    ")

	// Remove nolint comments to keep README examples clean
	extractedCode = filterNolintComments(extractedCode)

	return extractedCode, nil
}

// filterNolintComments removes //nolint comments from the code to keep
// README examples clean and focused on the API usage.
func filterNolintComments(code string) string {
	lines := strings.Split(code, "\n")
	var filteredLines []string

	for _, line := range lines {
		// Skip lines that are nolint comments
		if strings.Contains(line, "//nolint") {
			continue
		}
		filteredLines = append(filteredLines, line)
	}

	return strings.Join(filteredLines, "\n")
}

// findCodeBlock locates a Go code block within the README content after
// the specified start marker. It looks for the first ```go block
// after the start marker and returns the start and end positions of the
// entire code block including the backticks.
func findCodeBlock(content, startMarker string) (int, int, error) {
	startIdx := strings.Index(content, startMarker)
	if startIdx == -1 {
		return 0, 0, fmt.Errorf("start marker not found: %s", startMarker)
	}

	// Find the first ```go after the start marker
	codeStart := strings.Index(content[startIdx:], "```go")
	if codeStart == -1 {
		return 0, 0, fmt.Errorf("no Go code block found after: %s", startMarker)
	}
	codeStart += startIdx

	// Find the closing ``` for this code block
	codeEnd := strings.Index(content[codeStart+5:], "```")
	if codeEnd == -1 {
		return 0, 0, fmt.Errorf("no closing ``` found")
	}
	codeEnd += codeStart + 5

	return codeStart, codeEnd + 3, nil
}

// updateReadme processes the README.md file and replaces all configured
// code blocks with their corresponding example files. It iterates through
// all exampleMappings and updates each section with the current example code.
func updateReadme(readmePath string) error {
	content, err := os.ReadFile(readmePath)
	if err != nil {
		return err
	}

	readmeContent := string(content)

	for _, mapping := range exampleMappings {
		fmt.Printf("Processing: %s\n", mapping.ExamplePath)

		// Check if example file exists
		if _, err := os.Stat(mapping.ExamplePath); os.IsNotExist(err) {
			fmt.Printf("Warning: Example file not found: %s\n", mapping.ExamplePath)
			continue
		}

		// Extract Go code from example file
		goCode, err := extractGoCode(mapping.ExamplePath)
		if err != nil {
			fmt.Printf("Error extracting code from %s: %v\n", mapping.ExamplePath, err)
			continue
		}

		// Find the code block to replace
		startIdx, endIdx, err := findCodeBlock(readmeContent, mapping.StartMarker)
		if err != nil {
			fmt.Printf("Error finding code block for %s: %v\n", mapping.StartMarker, err)
			continue
		}

		// Replace the code block
		newCodeBlock := "```go\n" + goCode + "```"
		readmeContent = readmeContent[:startIdx] + newCodeBlock + readmeContent[endIdx:]

		fmt.Printf("Updated code block for: %s\n", mapping.StartMarker)
	}

	// Write back to README
	return os.WriteFile(readmePath, []byte(readmeContent), 0644)
}

// main is the entry point of the program. It validates the README.md file
// exists and calls updateReadme to perform the code block replacements.
func main() {
	if len(os.Args) > 1 {
		fmt.Println("Usage: go run update_examples.go")
		fmt.Println("This program updates the README.md file with actual example code from the examples/ directory.")
		return
	}

	readmePath := "README.md"

	// Check if README exists
	if _, err := os.Stat(readmePath); os.IsNotExist(err) {
		fmt.Printf("Error: %s not found\n", readmePath)
		os.Exit(1)
	}

	fmt.Println("Updating README.md with examples from examples/ directory...")

	if err := updateReadme(readmePath); err != nil {
		fmt.Printf("Error updating README: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("README.md updated successfully!")
}
