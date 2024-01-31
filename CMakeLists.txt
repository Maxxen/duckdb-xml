cmake_minimum_required(VERSION 2.8.12)

# Set extension name here
set(TARGET_NAME xml)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES src/xml_extension.cpp src/xlsx_reader.cpp
                      src/xml_reader.cpp src/zip_archive.cpp src/copy_xlsx.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

find_package(ZLIB REQUIRED)
find_package(EXPAT CONFIG REQUIRED)
find_package(minizip-ng CONFIG REQUIRED)

target_link_libraries(${EXTENSION_NAME} expat::expat MINIZIP::minizip-ng
                      ZLIB::ZLIB)
target_link_libraries(${LOADABLE_EXTENSION_NAME} expat::expat
                      MINIZIP::minizip-ng ZLIB::ZLIB)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
