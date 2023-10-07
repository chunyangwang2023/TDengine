aux_source_directory(src TRANSPORT_SRC)
IF (${TD_BUILD_MC})
  list(REMOVE_ITEM TRANSPORT_SRC src/thttp.c)
  list(REMOVE_ITEM TRANSPORT_SRC src/trans.c)
  list(REMOVE_ITEM TRANSPORT_SRC src/transCli.c)
  list(REMOVE_ITEM TRANSPORT_SRC src/transComm.c)
  list(REMOVE_ITEM TRANSPORT_SRC src/transSvr.c)
  list(APPEND TRANSPORT_SRC src/mc/transMc.c)
ENDIF ()
add_library(transport STATIC ${TRANSPORT_SRC})

target_include_directories(
    transport
    PUBLIC "${TD_SOURCE_DIR}/include/libs/transport"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    transport
    PUBLIC lz4_static
    PUBLIC os
    PUBLIC util
    PUBLIC common
    PUBLIC zlibstatic
)
if (${BUILD_WITH_UV_TRANS}) 
if (${BUILD_WITH_UV})
  target_link_libraries(
    transport    
    PUBLIC uv_a
  )
  add_definitions(-DUSE_UV) 
endif(${BUILD_WITH_UV})
endif(${BUILD_WITH_UV_TRANS})  

if (${BUILD_TEST})
  add_subdirectory(test)
endif(${BUILD_TEST})




  
