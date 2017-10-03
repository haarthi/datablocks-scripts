#!/usr/bin/env bash

# We're using ruby via rvm
#source /usr/share/rvm/environments/jruby-9.1.7.0
source /usr/share/rvm/environments/jruby-9.0.5.0

#export JRUBY_OPTS='-J-Xmx4000m -J-Xms4000m -J-XX:+UseG1GC' 
cd /home/lookerops/datablocks-etl/lib

jruby -J-Xmx2000m -J-Xms2000m  exchangerate.rb
