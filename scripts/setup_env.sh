#!/bin/sh

# tools for vscode go extension
echo "🚀 install & update gopls ..."
go install golang.org/x/tools/gopls@latest
echo "✅ done"

echo "🚀 install & update gotests ..."
go install github.com/cweill/gotests/gotests@latest
echo "✅ done"

echo "🚀 install & update impl ..."
go install github.com/josharian/impl@latest
echo "✅ done"

echo "🚀 install & update goplay ..."
go install github.com/haya14busa/goplay/cmd/goplay@latest
echo "✅ done"

echo "🚀 install & update dlv ..."
go install github.com/go-delve/delve/cmd/dlv@latest
echo "✅ done"

echo "🚀 install & update staticcheck ..."
go install honnef.co/go/tools/cmd/staticcheck@latest
echo "✅ done"

# tools for development
echo "🚀 install & update gofumpt ..."
go install mvdan.cc/gofumpt@latest
echo "✅ done"

echo "🚀 install & update golangci-lint ..."
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
echo "✅ done"

echo "🚀 install & update goimports ..."
go install golang.org/x/tools/cmd/goimports@latest
echo "✅ done"

echo "🚀 install & update mockgen ..."
go install go.uber.org/mock/mockgen@latest
echo "✅ done"

echo "🚀 install & update buf ..."
go install github.com/bufbuild/buf/cmd/buf@latest
echo "✅ done"

echo "🎉 setup tools complete"
