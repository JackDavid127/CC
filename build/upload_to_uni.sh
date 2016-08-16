#! /usr/bin/env sh

#
#  Do: ./authenticate_uni.sh ...before running this scrip.
#

remote_command () {
   ssh username@staff.ssh.inf.ed.ac.uk $1
} 

pushd . > /dev/null

cd ${0%/*}
cd ..

rm -rf ./package
mkdir ./package

cp -r ./build ./package
cp -r ./src ./package
cp -r ./lib ./package
cp  ./makefile ./package

tar --exclude=".*" -pczf package.tar.gz ./package
rm -rf package

scp ./package.tar.gz username@staff.ssh.inf.ed.ac.uk:~/ > /dev/null

remote_command "tar --warning=none -zxf ./package.tar.gz > /dev/null"
remote_command "rm -r ./package.tar.gz > /dev/null"

rm -rf ./package
rm package.tar.gz

popd > /dev/null
