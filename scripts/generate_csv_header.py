# this script generates the csv headers for the copy into test and the TPC-H dbgen
import os

csv_dir = 'test/sql/copy'


def get_csv_text(fpath, add_null_terminator=False):
    with open(fpath, 'rb') as f:
        text = bytearray(f.read())
    result_text = ""
    first = True
    for byte in text:
        if first:
            result_text += str(byte)
        else:
            result_text += ", " + str(byte)
        first = False
    if add_null_terminator:
        result_text += ", 0"
    return result_text


def write_csv(csv_dir, fname):
    result_text = get_csv_text(os.path.join(csv_dir, fname))
    fname = fname.replace(".csv", "").replace("-", "_")
    return "const uint8_t " + fname + '[] = {' + result_text + '};\n'


def write_binary(csv_dir, fname):
    result_text = get_csv_text(os.path.join(csv_dir, fname))
    fname = fname.split(".")[0].replace("-", "_")
    return "const uint8_t " + fname + '[] = {' + result_text + '};\n'


def create_csv_header(csv_dir):
    result = """/* THIS FILE WAS AUTOMATICALLY GENERATED BY generate_csv_header.py */

#pragma once

"""
    for fname in os.listdir(csv_dir):
        if fname.endswith(".csv"):
            result += write_csv(csv_dir, fname)
        elif fname.endswith(".csv.gz"):
            result += write_binary(csv_dir, fname)

    print(os.path.join(csv_dir, 'test_csv_header.hpp'))
    with open(os.path.join(csv_dir, 'test_csv_header.hpp'), 'w+') as f:
        f.write(result)


create_csv_header(csv_dir)

tpch_dir = 'third_party/dbgen'
tpch_queries = os.path.join(tpch_dir, 'queries')
tpch_answers_sf001 = os.path.join(tpch_dir, 'answers', 'sf0.01')
tpch_answers_sf01 = os.path.join(tpch_dir, 'answers', 'sf0.1')
tpch_answers_sf1 = os.path.join(tpch_dir, 'answers', 'sf1')
tpch_header = os.path.join(tpch_dir, 'include', 'tpch_constants.hpp')


def write_dir(dirname, varname):
    files = os.listdir(dirname)
    files.sort()
    result = ""
    aggregated_result = "const char *%s[] = {\n" % (varname,)
    for fname in files:
        file_varname = "%s_%s" % (varname, fname.split('.')[0])
        result += "const uint8_t %s[] = {" % (file_varname,) + get_csv_text(os.path.join(dirname, fname), True) + "};\n"
        aggregated_result += "\t(const char*) %s,\n" % (file_varname,)
    aggregated_result = aggregated_result[:-2] + "\n};\n"
    return result + aggregated_result


def create_tpch_header(tpch_dir):
    result = """/* THIS FILE WAS AUTOMATICALLY GENERATED BY generate_csv_header.py */

#pragma once

const int TPCH_QUERIES_COUNT = 32;

const char *TPCH_RAIS[] = {
    "CREATE PKFK RAI supplierInNation ON supplier (FROM s_nationkey REFERENCES nation.n_nationkey, TO s_nationkey REFERENCES nation.n_nationkey);",
    "CREATE PKFK RAI customerInNation ON customer (FROM c_nationkey REFERENCES nation.n_nationkey, TO c_nationkey REFERENCES nation.n_nationkey);",
    "CREATE PKFK RAI customerHasOrders ON orders (FROM o_custkey REFERENCES customer.c_custkey, TO o_custkey REFERENCES customer.c_custkey);",
    "CREATE PKFK RAI ordersHasLineitems ON lineitem (FROM l_orderkey REFERENCES orders.o_orderkey, TO l_orderkey REFERENCES orders.o_orderkey);",
    "CREATE PKFK RAI nationHasRegion ON nation (FROM n_regionkey REFERENCES region.r_regionkey, TO n_regionkey REFERENCES region.r_regionkey);",
    "CREATE UNDIRECTED RAI order_part ON lineitem (FROM l_orderkey REFERENCES orders.o_orderkey, TO l_partkey REFERENCES part.p_partkey);",
    "CREATE UNDIRECTED RAI order_supp ON lineitem (FROM l_orderkey REFERENCES orders.o_orderkey, TO l_suppkey REFERENCES supplier.s_suppkey);",
    "CREATE UNDIRECTED RAI supp_part ON partsupp (FROM ps_suppkey REFERENCES supplier.s_suppkey, TO ps_partkey REFERENCES part.p_partkey);"
};

"""
    # write the queries
    result += write_dir(tpch_queries, "TPCH_QUERIES")
    result += write_dir(tpch_answers_sf001, "TPCH_ANSWERS_SF0_01")
    result += write_dir(tpch_answers_sf01, "TPCH_ANSWERS_SF0_1")
    result += write_dir(tpch_answers_sf1, "TPCH_ANSWERS_SF1")

    with open(tpch_header, 'w+') as f:
        f.write(result)


print(tpch_header)
create_tpch_header(tpch_dir)
