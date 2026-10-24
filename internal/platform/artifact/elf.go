package artifact

import (
	"bytes"
	"debug/elf"
	"errors"
	"fmt"
)

// ErrPlatform means the ELF header does not name a Go os/arch (E09-D037).
var ErrPlatform = errors.New("artifact: unsupported ELF os/arch")

// elfArch is one row of Go's ELF machine, class and endianness (E09-D037).
type elfArch struct {
	machine elf.Machine
	class   elf.Class
	data    elf.Data
	arch    string
}

// Go's linux linker writes ELFOSABI_NONE. The other three are the OSABI values
// that linker sets for freebsd, netbsd and openbsd.
var elfArches = []elfArch{
	{elf.EM_X86_64, elf.ELFCLASS64, elf.ELFDATA2LSB, "amd64"},
	{elf.EM_AARCH64, elf.ELFCLASS64, elf.ELFDATA2LSB, "arm64"},
	{elf.EM_386, elf.ELFCLASS32, elf.ELFDATA2LSB, "386"},
	{elf.EM_ARM, elf.ELFCLASS32, elf.ELFDATA2LSB, "arm"},
	{elf.EM_RISCV, elf.ELFCLASS64, elf.ELFDATA2LSB, "riscv64"},
	{elf.EM_PPC64, elf.ELFCLASS64, elf.ELFDATA2LSB, "ppc64le"},
	{elf.EM_PPC64, elf.ELFCLASS64, elf.ELFDATA2MSB, "ppc64"},
	{elf.EM_S390, elf.ELFCLASS64, elf.ELFDATA2MSB, "s390x"},
}

func elfPlatform(binary []byte) (osName, arch string, err error) {
	f, err := elf.NewFile(bytes.NewReader(binary))
	if err != nil {
		return "", "", fmt.Errorf("%w: %v", ErrPlatform, err)
	}
	defer f.Close()
	switch f.OSABI {
	case elf.ELFOSABI_NONE, elf.ELFOSABI_LINUX:
		osName = "linux"
	case elf.ELFOSABI_FREEBSD:
		osName = "freebsd"
	case elf.ELFOSABI_NETBSD:
		osName = "netbsd"
	case elf.ELFOSABI_OPENBSD:
		osName = "openbsd"
	default:
		return "", "", fmt.Errorf("%w: OSABI %d", ErrPlatform, f.OSABI)
	}
	for _, row := range elfArches {
		if f.Machine == row.machine && f.Class == row.class && f.Data == row.data {
			return osName, row.arch, nil
		}
	}
	return "", "", fmt.Errorf("%w: machine %s class %s data %s", ErrPlatform, f.Machine, f.Class, f.Data)
}
