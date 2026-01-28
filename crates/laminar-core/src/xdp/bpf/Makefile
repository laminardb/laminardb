# LaminarDB XDP Program Build System
#
# Build the XDP program for CPU steering and packet filtering.
#
# Requirements:
#   - clang (with BPF target)
#   - llvm-strip (optional, for stripping debug info)
#   - libbpf-dev (for headers)
#
# Usage:
#   make          - Build the XDP program
#   make install  - Install to /usr/share/laminardb/
#   make clean    - Remove build artifacts
#   make test     - Load/unload test on loopback

CLANG ?= clang
LLC ?= llc
LLVM_STRIP ?= llvm-strip

# BPF compilation flags
BPF_CFLAGS := -O2 \
	-target bpf \
	-g \
	-D__TARGET_ARCH_$(shell uname -m | sed 's/x86_64/x86/' | sed 's/aarch64/arm64/') \
	-Wall \
	-Wno-unused-value \
	-Wno-pointer-sign \
	-Wno-compare-distinct-pointer-types

# Include paths (adjust for your system)
BPF_INCLUDES := -I/usr/include \
	-I/usr/include/bpf

# Source and output
XDP_SRC := laminar_xdp.c
XDP_OBJ := laminar_xdp.o
XDP_STRIPPED := laminar_xdp.stripped.o

# Installation directory
INSTALL_DIR := /usr/share/laminardb

.PHONY: all clean install uninstall test help

all: $(XDP_OBJ)

$(XDP_OBJ): $(XDP_SRC)
	@echo "Building XDP program..."
	$(CLANG) $(BPF_CFLAGS) $(BPF_INCLUDES) -c $< -o $@
	@echo "Built: $@"

$(XDP_STRIPPED): $(XDP_OBJ)
	@echo "Stripping debug info..."
	$(LLVM_STRIP) -g $< -o $@ 2>/dev/null || cp $< $@

install: $(XDP_OBJ)
	@echo "Installing to $(INSTALL_DIR)..."
	install -D -m 644 $(XDP_OBJ) $(INSTALL_DIR)/$(XDP_OBJ)
	@echo "Installed: $(INSTALL_DIR)/$(XDP_OBJ)"

uninstall:
	rm -f $(INSTALL_DIR)/$(XDP_OBJ)

clean:
	rm -f $(XDP_OBJ) $(XDP_STRIPPED)

# Test loading/unloading on loopback interface
test: $(XDP_OBJ)
	@echo "Testing XDP load/unload on loopback..."
	@if [ $$(id -u) -ne 0 ]; then \
		echo "Error: Must run as root"; \
		exit 1; \
	fi
	ip link set dev lo xdpgeneric obj $(XDP_OBJ) sec xdp
	@echo "XDP loaded successfully"
	ip link set dev lo xdpgeneric off
	@echo "XDP unloaded successfully"
	@echo "Test passed!"

# Show XDP stats (requires bpftool)
stats:
	@if command -v bpftool >/dev/null 2>&1; then \
		bpftool map dump name stats; \
	else \
		echo "bpftool not installed"; \
	fi

# Verify BPF program (requires llvm-objdump)
verify: $(XDP_OBJ)
	@echo "Verifying BPF program..."
	llvm-objdump -S $(XDP_OBJ) || objdump -S $(XDP_OBJ)

help:
	@echo "LaminarDB XDP Build System"
	@echo ""
	@echo "Targets:"
	@echo "  all       - Build the XDP program (default)"
	@echo "  install   - Install to $(INSTALL_DIR)"
	@echo "  uninstall - Remove from $(INSTALL_DIR)"
	@echo "  clean     - Remove build artifacts"
	@echo "  test      - Test load/unload on loopback (requires root)"
	@echo "  stats     - Show XDP statistics (requires bpftool)"
	@echo "  verify    - Disassemble BPF program"
	@echo ""
	@echo "Variables:"
	@echo "  CLANG=$(CLANG)"
	@echo "  INSTALL_DIR=$(INSTALL_DIR)"
