CATEGORIES = {"sofa": "소파", "bed": "침대", "desk": "책상", "plant": "화분", "lamp": "조명", "other": "기타"}
# shortcut: local sample catalog, replace with a real product API when recommendation work starts.
PRODUCTS = [
    {"id": "sofa-01", "category": "sofa", "name": "모듈 소파", "price": 390000, "color": "#c7b69b", "width": 0.30, "height": 0.18},
    {"id": "bed-01", "category": "bed", "name": "낮은 침대", "price": 450000, "color": "#d3c5ae", "width": 0.25, "height": 0.32},
    {"id": "desk-01", "category": "desk", "name": "원목 책상", "price": 150000, "color": "#b68962", "width": 0.25, "height": 0.15},
    {"id": "plant-01", "category": "plant", "name": "초록 화분", "price": 35000, "color": "#508b68", "width": 0.13, "height": 0.22},
    {"id": "lamp-01", "category": "lamp", "name": "플로어 조명", "price": 69000, "color": "#d8b567", "width": 0.12, "height": 0.25},
]
PRODUCT_IDS = {product["id"] for product in PRODUCTS}
