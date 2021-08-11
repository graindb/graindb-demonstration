//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

//! The Serialize class is a base class that can be used to serializing objects into a binary buffer
class Serializer {
public:
	virtual ~Serializer() {
	}

	virtual void WriteData(const_data_ptr_t buffer, idx_t write_size) = 0;

	template <class T> void Write(T element) {
		WriteData((const_data_ptr_t)&element, sizeof(T));
	}

	void WriteString(const string &val) {
		assert(val.size() <= std::numeric_limits<uint32_t>::max());
		Write<uint32_t>((uint32_t)val.size());
		if (val.size() > 0) {
			WriteData((const_data_ptr_t)val.c_str(), val.size());
		}
	}

	template <class T> void WriteList(vector<unique_ptr<T>> &list) {
		assert(list.size() <= std::numeric_limits<uint32_t>::max());
		Write<uint32_t>((uint32_t)list.size());
		for (auto &child : list) {
			child->Serialize(*this);
		}
	}

	template <class T> void WriteOptional(unique_ptr<T> &element) {
		Write<bool>(element ? true : false);
		if (element) {
			element->Serialize(*this);
		}
	}
};

//! The Deserializer class assists in deserializing a binary blob back into an
//! object
class Deserializer {
public:
	virtual ~Deserializer() {
	}

	//! Reads [read_size] bytes into the buffer
	virtual void ReadData(data_ptr_t buffer, idx_t read_size) = 0;

	template <class T> T Read() {
		T value;
		ReadData((data_ptr_t)&value, sizeof(T));
		return value;
	}

	template <class T> void ReadList(vector<unique_ptr<T>> &list) {
		auto select_count = Read<uint32_t>();
		for (uint32_t i = 0; i < select_count; i++) {
			auto child = T::Deserialize(*this);
			list.push_back(move(child));
		}
	}

	template <class T> unique_ptr<T> ReadOptional() {
		auto has_entry = Read<bool>();
		if (has_entry) {
			return T::Deserialize(*this);
		}
		return nullptr;
	}
};

template <> string Deserializer::Read();

} // namespace duckdb
