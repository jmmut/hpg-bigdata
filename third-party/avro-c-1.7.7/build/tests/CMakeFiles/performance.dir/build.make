# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 2.8

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list

# Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/jtarraga/appl-jtarraga/hpg-bigdata/third-party/avro-c-1.7.7

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/jtarraga/appl-jtarraga/hpg-bigdata/third-party/avro-c-1.7.7/build

# Include any dependencies generated for this target.
include tests/CMakeFiles/performance.dir/depend.make

# Include the progress variables for this target.
include tests/CMakeFiles/performance.dir/progress.make

# Include the compile flags for this target's objects.
include tests/CMakeFiles/performance.dir/flags.make

tests/CMakeFiles/performance.dir/performance.o: tests/CMakeFiles/performance.dir/flags.make
tests/CMakeFiles/performance.dir/performance.o: ../tests/performance.c
	$(CMAKE_COMMAND) -E cmake_progress_report /home/jtarraga/appl-jtarraga/hpg-bigdata/third-party/avro-c-1.7.7/build/CMakeFiles $(CMAKE_PROGRESS_1)
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Building C object tests/CMakeFiles/performance.dir/performance.o"
	cd /home/jtarraga/appl-jtarraga/hpg-bigdata/third-party/avro-c-1.7.7/build/tests && /usr/bin/gcc  $(C_DEFINES) $(C_FLAGS) -o CMakeFiles/performance.dir/performance.o   -c /home/jtarraga/appl-jtarraga/hpg-bigdata/third-party/avro-c-1.7.7/tests/performance.c

tests/CMakeFiles/performance.dir/performance.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing C source to CMakeFiles/performance.dir/performance.i"
	cd /home/jtarraga/appl-jtarraga/hpg-bigdata/third-party/avro-c-1.7.7/build/tests && /usr/bin/gcc  $(C_DEFINES) $(C_FLAGS) -E /home/jtarraga/appl-jtarraga/hpg-bigdata/third-party/avro-c-1.7.7/tests/performance.c > CMakeFiles/performance.dir/performance.i

tests/CMakeFiles/performance.dir/performance.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling C source to assembly CMakeFiles/performance.dir/performance.s"
	cd /home/jtarraga/appl-jtarraga/hpg-bigdata/third-party/avro-c-1.7.7/build/tests && /usr/bin/gcc  $(C_DEFINES) $(C_FLAGS) -S /home/jtarraga/appl-jtarraga/hpg-bigdata/third-party/avro-c-1.7.7/tests/performance.c -o CMakeFiles/performance.dir/performance.s

tests/CMakeFiles/performance.dir/performance.o.requires:
.PHONY : tests/CMakeFiles/performance.dir/performance.o.requires

tests/CMakeFiles/performance.dir/performance.o.provides: tests/CMakeFiles/performance.dir/performance.o.requires
	$(MAKE) -f tests/CMakeFiles/performance.dir/build.make tests/CMakeFiles/performance.dir/performance.o.provides.build
.PHONY : tests/CMakeFiles/performance.dir/performance.o.provides

tests/CMakeFiles/performance.dir/performance.o.provides.build: tests/CMakeFiles/performance.dir/performance.o

# Object files for target performance
performance_OBJECTS = \
"CMakeFiles/performance.dir/performance.o"

# External object files for target performance
performance_EXTERNAL_OBJECTS =

tests/performance: tests/CMakeFiles/performance.dir/performance.o
tests/performance: src/libavro.a
tests/performance: /usr/lib/x86_64-linux-gnu/libz.so
tests/performance: tests/CMakeFiles/performance.dir/build.make
tests/performance: tests/CMakeFiles/performance.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --red --bold "Linking C executable performance"
	cd /home/jtarraga/appl-jtarraga/hpg-bigdata/third-party/avro-c-1.7.7/build/tests && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/performance.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
tests/CMakeFiles/performance.dir/build: tests/performance
.PHONY : tests/CMakeFiles/performance.dir/build

tests/CMakeFiles/performance.dir/requires: tests/CMakeFiles/performance.dir/performance.o.requires
.PHONY : tests/CMakeFiles/performance.dir/requires

tests/CMakeFiles/performance.dir/clean:
	cd /home/jtarraga/appl-jtarraga/hpg-bigdata/third-party/avro-c-1.7.7/build/tests && $(CMAKE_COMMAND) -P CMakeFiles/performance.dir/cmake_clean.cmake
.PHONY : tests/CMakeFiles/performance.dir/clean

tests/CMakeFiles/performance.dir/depend:
	cd /home/jtarraga/appl-jtarraga/hpg-bigdata/third-party/avro-c-1.7.7/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/jtarraga/appl-jtarraga/hpg-bigdata/third-party/avro-c-1.7.7 /home/jtarraga/appl-jtarraga/hpg-bigdata/third-party/avro-c-1.7.7/tests /home/jtarraga/appl-jtarraga/hpg-bigdata/third-party/avro-c-1.7.7/build /home/jtarraga/appl-jtarraga/hpg-bigdata/third-party/avro-c-1.7.7/build/tests /home/jtarraga/appl-jtarraga/hpg-bigdata/third-party/avro-c-1.7.7/build/tests/CMakeFiles/performance.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : tests/CMakeFiles/performance.dir/depend
