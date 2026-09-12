# Re-render the diagrams of this design doc (*.mmd -> *.png).

MMDC ?= npx -y @mermaid-js/mermaid-cli@11.17.0
DIAGRAMS := $(patsubst %.mmd,%.png,$(wildcard *.mmd))

.PHONY: all
all: $(DIAGRAMS)

%.png: %.mmd
	$(MMDC) -i $< -o $@ -w 2000 -s 2 -b white
