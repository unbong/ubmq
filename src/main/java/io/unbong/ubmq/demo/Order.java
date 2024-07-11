package io.unbong.ubmq.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Description
 *
 * @author <a href="ecunbong@gmail.com">unbong</a>
 * 2024-07-04 19:42
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order {
    private long id;
    private String name;
    private double price;
}
