#include <unistd.h>
#include <fcntl.h>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <db/Utility.h>
#include <db/HeapFile.h>

uint8_t page[4096] = {
        // Page header 64B
        0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        // 20 tuples
        0xbd, 0x7c, 0x00, 0x00, 0x5e, 0x03, 0x00, 0x00,
        0xda, 0x72, 0x00, 0x00, 0x33, 0xde, 0x00, 0x00,
        0xbc, 0x05, 0x00, 0x00, 0xc1, 0x16, 0x00, 0x00,
        0xd4, 0x45, 0x00, 0x00, 0x36, 0xcc, 0x00, 0x00,
        0xce, 0x18, 0x00, 0x00, 0xfa, 0x8c, 0x00, 0x00,
        0xe0, 0x87, 0x00, 0x00, 0xfb, 0xaa, 0x00, 0x00,
        0xc9, 0x6f, 0x00, 0x00, 0x2a, 0xde, 0x00, 0x00,
        0x09, 0x4b, 0x00, 0x00, 0xd5, 0x5a, 0x00, 0x00,
        0x8e, 0xdc, 0x00, 0x00, 0x93, 0x61, 0x00, 0x00,
        0xf0, 0xc8, 0x00, 0x00, 0x6d, 0xdd, 0x00, 0x00,
        0x0c, 0x0e, 0x00, 0x00, 0x63, 0xf3, 0x00, 0x00,
        0x01, 0xb2, 0x00, 0x00, 0x9f, 0x0a, 0x00, 0x00,
        0x30, 0x56, 0x00, 0x00, 0x37, 0xaa, 0x00, 0x00,
        0x3c, 0xa7, 0x00, 0x00, 0x93, 0xaf, 0x00, 0x00,
        0xad, 0x56, 0x00, 0x00, 0x0c, 0x4d, 0x00, 0x00,
        0x0d, 0x83, 0x00, 0x00, 0xca, 0x8e, 0x00, 0x00,
        0x7e, 0x23, 0x00, 0x00, 0xc0, 0xcf, 0x00, 0x00,
        0x7e, 0xa7, 0x00, 0x00, 0x72, 0x82, 0x00, 0x00,
        0x3a, 0xf5, 0x00, 0x00, 0x82, 0x52, 0x00, 0x00,
        0x2d, 0x43, 0x00, 0x00, 0x04, 0x40, 0x00, 0x00,
        // rest is empty
};
uint8_t page2[4096] = {
    0xff, 0xff, 0x0f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    // 20 tuples
    0xbd, 0x7c, 0x00, 0x00, 0x5e, 0x03, 0x00, 0x00,
    0xda, 0x72, 0x00, 0x00, 0x33, 0xde, 0x00, 0x00,
    0xbc, 0x05, 0x00, 0x00, 0xc1, 0x16, 0x00, 0x00,
    0xd4, 0x45, 0x00, 0x00, 0x36, 0xcc, 0x00, 0x00,
    0xce, 0x18, 0x00, 0x00, 0xfa, 0x8c, 0x00, 0x00,
    0xe0, 0x87, 0x00, 0x00, 0xfb, 0xaa, 0x00, 0x00,
    0xc9, 0x6f, 0x00, 0x00, 0x2a, 0xde, 0x00, 0x00,
    0x09, 0x4b, 0x00, 0x00, 0xd5, 0x5a, 0x00, 0x00,
    0x8e, 0xdc, 0x00, 0x00, 0x93, 0x61, 0x00, 0x00,
    0xf0, 0xc8, 0x00, 0x00, 0x6d, 0xdd, 0x00, 0x00,
    0x0c, 0x0e, 0x00, 0x00, 0x63, 0xf3, 0x00, 0x00,
    0x01, 0xb2, 0x00, 0x00, 0x9f, 0x0a, 0x00, 0x00,
    0x30, 0x56, 0x00, 0x00, 0x37, 0xaa, 0x00, 0x00,
    0x3c, 0xa7, 0x00, 0x00, 0x93, 0xaf, 0x00, 0x00,
    0xad, 0x56, 0x00, 0x00, 0x0c, 0x4d, 0x00, 0x00,
    0x0d, 0x83, 0x00, 0x00, 0xca, 0x8e, 0x00, 0x00,
    0x7e, 0x23, 0x00, 0x00, 0xc0, 0xcf, 0x00, 0x00,
    0x7e, 0xa7, 0x00, 0x00, 0x72, 0x82, 0x00, 0x00,
    0x3a, 0xf5, 0x00, 0x00, 0x82, 0x52, 0x00, 0x00,
    0x2d, 0x43, 0x00, 0x00, 0x04, 0x40, 0x00, 0x00,};

void create_page(int fd, off_t offset, uint8_t byte) {
    page[0] = byte;
    ssize_t bytes_written = pwrite(fd, page2, 4096, offset);
    if (bytes_written == -1) {
        perror("pwrite");
        exit(1);
    }
    printf("bytes_written: %zd\n", bytes_written);
}

void read_page(int fd, uint8_t *data, off_t offset) {
    ssize_t bytes_read = pread(fd, data, 4096, offset);
    if (bytes_read == -1) {
        perror("pread");
        exit(1);
    }
    printf("bytes_read: %zd\n", bytes_read);
}

void print_page(uint8_t *data) {
    if (false) {
        db::HeapFile hf("test.dat", db::Utility::getTupleDesc(2));
        for (const auto &item: hf) {
            std::cout << item.getField(0).to_string() << ' ' << item.getField(1).to_string() << std::endl;
        }
        std::cout << std::endl;
        return;
    }
    for (int i = 0; i < 200; ++i) {
        printf("0x%02x", data[i]);
        if (i % 16 == 15) {
            printf("\n");
        } else {
            printf(" ");
        }
    }
    printf("\n");
}

int main() {
    int fd = open("heapfile.dat", O_RDWR | O_CREAT, 0644);
    if (fd == -1) {
        perror("open");
        return 1;
    }

    for (int i = 0; i < 3; ++i) {
        create_page(fd, 4096 * i, 0xff - i);
    }

    for (int i = 0; i < 3; ++i) {
        uint8_t data[4096];
        read_page(fd, data, 4096 * i);
        print_page(data);
    }

    close(fd);
    return 0;
}
