package com.example.timedeal.domain.product.repository;

import com.example.timedeal.domain.product.entity.Product;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductRepository extends JpaRepository<Product,Long> {
}
