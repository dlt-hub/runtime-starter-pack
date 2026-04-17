WORKSPACES := $(patsubst %/pyproject.toml,%,$(wildcard */pyproject.toml))

.PHONY: sync $(WORKSPACES)

sync: $(WORKSPACES)

$(WORKSPACES):
	@echo "==> uv sync in $@"
	cd $@ && uv sync
