package pack1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Custom implements Writable {


    String symbol;
    String exchange;
    double stockOpen;
    double stockHigh;
    double stockLow;
    double stockClose;
    double stockVolume;
    double stockAdjClose;

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public double getStockOpen() {
        return stockOpen;
    }

    public void setStockOpen(double stockOpen) {
        this.stockOpen = stockOpen;
    }

    public double getStockHigh() {
        return stockHigh;
    }

    public void setStockHigh(double stockHigh) {
        this.stockHigh = stockHigh;
    }

    public double getStockLow() {
        return stockLow;
    }

    public void setStockLow(double stockLow) {
        this.stockLow = stockLow;
    }

    public double getStockClose() {
        return stockClose;
    }

    public void setStockClose(double stockClose) {
        this.stockClose = stockClose;
    }

    public double getStockVolume() {
        return stockVolume;
    }

    public void setStockVolume(double stockVolume) {
        this.stockVolume = stockVolume;
    }

    public double getStockAdjClose() {
        return stockAdjClose;
    }

    public void setStockAdjClose(double stockAdjClose) {
        this.stockAdjClose = stockAdjClose;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(stockLow);
        dataOutput.writeDouble(stockHigh);
        dataOutput.writeDouble(stockOpen);
        dataOutput.writeDouble(stockClose);
        dataOutput.writeDouble(stockVolume);
        dataOutput.writeDouble(stockAdjClose);
        dataOutput.writeUTF(exchange);
        dataOutput.writeUTF(symbol);


    }

    @Override
    public String toString() {
        return "StockInfo [exchange=" + exchange + ", stockSymbol=" + symbol + ", stockOpenPrice=" + stockOpen
                + ", stockPriceHigh=" + stockHigh + ", stockPriceLow=" + stockLow + ", stockPriceClosed="
                + stockClose + ", stockVolume=" + stockVolume + "]";
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        stockLow=dataInput.readDouble();
        stockHigh=dataInput.readDouble();
        stockOpen=dataInput.readDouble();
        stockClose=dataInput.readDouble();
        stockVolume = dataInput.readDouble();
        stockAdjClose=dataInput.readDouble();
        exchange= dataInput.readUTF();
        symbol=dataInput.readUTF();


    }
}
