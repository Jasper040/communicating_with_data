CREATE TABLE IF NOT EXISTS public.inventory (
    item_no            text,
    item               text,
    colour_no          integer,
    colour             text,
    size               text,
    barcode            text,
    stock              integer
);

CREATE TABLE IF NOT EXISTS public.products (
    item_no            text,
    item               text,
    item_group         text,
    sub_item_group     text,
    vendor             text,
    country_of_origin  text,
    brand              text,
    fit                text,
    size_group         text,
    season             text,
    sales_listprice    numeric(12,2),
    colour_no          integer,
    colour             text,
    size               text,
    barcode            text
);

CREATE TABLE IF NOT EXISTS public.total_sales_b2c (
    order_no                   integer,
    order_date                 date,
    item_no                    text,
    item                       text,
    brand                      text,
    quantity                   integer,
    amount_lcy_discount        numeric(12,2),
    season                     text,
    colour_no                  integer,
    colour                     text,
    size                       text,
    barcode                    text,
    webshop                    text,
    channel                    text,
    shipping_costs             numeric(12,2),
    reference                  text,
    direct_costs               numeric(12,2)
);

CREATE TABLE IF NOT EXISTS public.purchased (
    purchase_order_id          integer,
    line_id                    integer,
    type                       text,
    status                     text,
    vendor                     text,
    warehouse                  text,
    season                     text,
    currency                   text,
    order_date                 date,
    etd                        date,
    eta                        date,
    dispatch_method            text,
    discount_percentage        numeric(5,2),
    reference                  text,
    item_no                    text,
    item                       text,
    colour_no                  integer,
    colour                     text,
    size                       text,
    barcode                    text,
    quantity                   integer,
    quantity_received          integer,
    quantity_to_receive        integer,
    content                    integer,
    colour_etd                 date,
    colour_eta                 date,
    purchase_price             numeric(12,2)
);