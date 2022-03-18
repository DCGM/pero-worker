#!/bin/sh

apt_dep=./debian_dependencies.txt
python_dep=./requirements.txt

dependencies=""


if [ ! -f "$apt_dep" ]; then
    echo 'debian_dependencies.txt not present in current directory!' >&2
    exit 1
fi

# remove comments
tmp_dependencies=$(grep -v "^#" "$apt_dep");
for line in $tmp_dependencies; do
    if [ -n "$line" ]; then
        dependencies="$line $dependencies"
    fi
done

dependencies="$dependencies python3-venv"

echo 'Installing dependencies via apt'
if [ 0 -ne "$(id -u)" ]; then
    sudo apt-get install -y $dependencies
else
    apt-get install -y $dependencies
fi

rc=$?

if [ $rc -ne 0 ]; then
    echo 'Installation failed!' >&2
    exit $rc
fi

echo 'Installing dependencies via pip'
python3 -m venv .venv
. ./.venv/bin/activate
pip install --upgrade pip
pip3 install --upgrade --requirement "$python_dep"

rc=$?

if [ $rc -ne 0 ]; then
    echo 'Installation failed!' >&2
    exit $rc
fi

if [ -z "$PYTHONPATH" ]; then
    echo 'export PYTHONPATH="$(dirname $VIRTUAL_ENV)/libs:$(dirname $VIRTUAL_ENV)/pero-ocr"' >> ./.venv/bin/activate
else
    echo 'export PYTHONPATH="$PYTHONPATH:$(dirname $VIRTUAL_ENV)/libs:$(dirname $VIRTUAL_ENV)/pero-ocr"' >> ./.venv/bin/activate
fi

exit 0
