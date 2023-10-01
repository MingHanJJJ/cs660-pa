#include <db/Catalog.h>
#include <db/TupleDesc.h>
#include <stdexcept>

using namespace db;

void Catalog::addTable(DbFile *file, const std::string &name, const std::string &pkeyField) {
    // TODO pa1.2: implement
    tables[file->getId()] = new Table(file, name, pkeyField);
}

int Catalog::getTableId(const std::string &name) const {
    // TODO pa1.2: implement
    for(auto table : tables){
        if(table.second->name == name){
            return table.first;
        }
    }
    throw std::invalid_argument("input name is invalid");
}

const TupleDesc &Catalog::getTupleDesc(int tableid) const {
    // TODO pa1.2: implement
    return tables.find(tableid)->second->file->getTupleDesc();
}

DbFile *Catalog::getDatabaseFile(int tableid) const {
    // TODO pa1.2: implement
    return tables.find(tableid)->second->file;
}

std::string Catalog::getPrimaryKey(int tableid) const {
    // TODO pa1.2: implement
    return tables.find(tableid)->second->pkeyField;
}

std::string Catalog::getTableName(int id) const {
    // TODO pa1.2: implement
    return tables.find(id)->second->name;
}

void Catalog::clear() {
    // TODO pa1.2: implement
    tables.clear();
}
