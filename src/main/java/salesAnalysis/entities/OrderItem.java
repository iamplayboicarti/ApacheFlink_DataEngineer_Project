package salesAnalysis.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Getter;

@Data
@NoArgsConstructor
@Getter
public class OrderItem {
    public int orderItemId;
    public int orderId;
    public int productId;
    public int quantity;
    public float pricePerUnit;

    public OrderItem(int orderItemId, int orderId, int productId, int quantity, float pricePerUnit) {
        this.orderItemId = orderItemId;
        this.orderId = orderId;
        this.productId = productId;
        this.quantity = quantity;
        this.pricePerUnit = pricePerUnit;
    }
}
