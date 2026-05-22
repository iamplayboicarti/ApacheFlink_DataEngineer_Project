package salesAnalysis.entities;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@Getter
public class Product {
    public Integer productId;
    public String name;
    public String description;
    public double price;
    public String category;

    public Product(Integer productId, String name, String description, double price, String category) {
        this.productId = productId;
        this.name = name;
        this.description = description;
        this.price = price;
        this.category = category;
    }
}