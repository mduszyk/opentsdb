# Copyright (C) 2011-2012  The OpenTSDB Authors.
#
# This library is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 2.1 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library.  If not, see <http://www.gnu.org/licenses/>.

ASYNCHBASE_VERSION := 1.4.1
ASYNCHBASE := third_party/hbase/asynchbase-$(ASYNCHBASE_VERSION).jar
ASYNCHBASE_BASE_URL := $(OPENTSDB_THIRD_PARTY_BASE_URL)

#$(ASYNCHBASE): $(ASYNCHBASE).md5
#	set dummy "$(ASYNCHBASE_BASE_URL)" "$(ASYNCHBASE)"; shift; $(FETCH_DEPENDENCY)

$(ASYNCHBASE):
	echo "!!! asynchbase with filter support required please place asynchbase-$(ASYNCHBASE_VERSION).jar in build/third_party/hbase/ !!!"

THIRD_PARTY += $(ASYNCHBASE)


HBASE_VERSION := 0.92.2
HBASE := third_party/hbase/hbase-$(HBASE_VERSION).jar

$(HBASE):
	echo "!!! hbase required please place hbase-$(HBASE_VERSION).jar in build/third_party/hbase/ !!!"

THIRD_PARTY += $(HBASE)

HADOOP_VERSION := 1.0.4
HADOOP := third_party/hbase/hadoop-core-$(HADOOP_VERSION).jar

$(HADOOP):
	echo "!!! hadoop required please place hadoop-core-$(HADOOP_VERSION).jar in build/third_party/hbase/ !!!"

THIRD_PARTY += $(HADOOP)

