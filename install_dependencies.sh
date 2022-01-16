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

if [ "$1" = 'venv' ]; then
    dependencies="$dependencies python3-venv"
fi

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
if [ "$1" = 'venv' ]; then
    python3 -m venv .venv
    . ./.venv/bin/activate
    pip install --upgrade pip
fi
pip3 install --upgrade --requirement "$python_dep"

rc=$?

if [ $rc -ne 0 ]; then
    echo 'Installation failed!' >&2
    exit $rc
fi

exit 0
