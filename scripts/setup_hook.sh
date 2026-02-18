#!/bin/sh

SOURCE_COMMIT=./scripts/git/pre-commit
TARGET_COMMIT=.git/hooks/pre-commit

SOURCE_PUSH=./scripts/git/pre-push
TARGET_PUSH=.git/hooks/pre-push

echo "🔧 setup git pre-commit hooks ..."
cp $SOURCE_COMMIT $TARGET_COMMIT
echo "✅ done"

echo "start setup git pre-push hooks ..."
cp $SOURCE_PUSH $TARGET_PUSH
echo "✅ done"

echo "🔧 make git pre-commit and pre-push hooks executable ..."
test -x $TARGET_COMMIT || chmod +x $TARGET_COMMIT
test -x $TARGET_PUSH || chmod +x $TARGET_PUSH
echo "✅ done"

echo "🎉 setup git hooks complete"
