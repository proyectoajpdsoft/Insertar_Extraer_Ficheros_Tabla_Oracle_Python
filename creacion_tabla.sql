
CREATE TABLE factura (
    codigo NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fecha DATE DEFAULT SYSDATE,
    cliente VARCHAR(150),
    importe DECIMAL(10,2),
    factura BLOB
);