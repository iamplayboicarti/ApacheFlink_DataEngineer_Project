package salesAnalysis.dto;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@Getter
public class CategorySalesDTO {
    public String category;
    public float totalSales;
    public int count;

    public CategorySalesDTO (String category, float totalSales, int count) {
        this.category = category;
        this.totalSales = totalSales;
        this.count = count;
    }

    public String getCategory() {
        return category;
    }
}
