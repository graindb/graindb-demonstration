add_library_unity(duckdb_rai_execution OBJECT alists.cpp rel_adj_index.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_rai_execution>
        PARENT_SCOPE)
