package salesAnalysis;

import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import salesAnalysis.entities.OrderItem;
import salesAnalysis.entities.Product;

import java.util.HashMap;
import java.util.Map;

public class ProductBroadcastJoin extends BroadcastProcessFunction<OrderItem, Product, Tuple6<String, String, Float, Integer, Float, String>> {

    private final Map<Integer, Product> productMap = new HashMap<>();

    @Override
    public void processElement(OrderItem orderItem, ReadOnlyContext ctx, Collector<Tuple6<String, String, Float, Integer, Float, String>> out) {
        Product product = productMap.get(orderItem.productId);  // <-- FIX ở đây
        if (product != null) {
            out.collect(new Tuple6<>(
                    String.valueOf(product.productId),
                    product.name,
                    orderItem.pricePerUnit,
                    orderItem.quantity,
                    orderItem.pricePerUnit * orderItem.quantity,
                    product.category
            ));
            System.out.println("📌 Broadcast product loaded: " + product.productId + " - " + product.name);
        } else {
            System.out.println("⚠️ No match for productId: " + orderItem.productId);
        }
    }

    @Override
    public void processBroadcastElement(Product product, Context ctx, Collector<Tuple6<String, String, Float, Integer, Float, String>> out) {
        productMap.put(product.productId, product);
        System.out.println("📌 Broadcast product loaded: " + product.productId + " - " + product.name);
    }
}
