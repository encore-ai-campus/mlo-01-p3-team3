from interior_recommender.catalog_metadata import product_metadata


def test_metadata_keeps_conflicting_prices_and_component_dimensions_unknown():
    row = {"product_id": "501", "name": "침대", "categories": [{"category_name": "침대"}],
           "listing_prices": [{"amount": amount, "kind": "comparison_lowest", "currency": "KRW"} for amount in (10, 20)],
           "specifications": [{"name": "매트(가로x세로x높이)", "value": "110x200x25cm"}]}
    metadata = product_metadata(row, "test-source")
    assert metadata["price_krw"] is None and metadata["width_mm"] is None
    assert metadata["category_code"] == "BED"


def test_dimensions_are_independent_and_ranges_are_not_collapsed_to_endpoints():
    row = {"product_id": "1", "name": "책상", "categories": [{"category_name": "책상"}],
           "specifications": [{"name": "크기(가로x세로x높이)", "value": "1600x800x675~810mm"}]}
    metadata = product_metadata(row, "source")
    assert metadata["width_mm"] == 1600 and metadata["depth_mm"] == 800
    assert metadata["height_mm"] is None
    assert metadata["dimension_ranges_mm"]["height_mm"] == {"minimum": 675, "maximum": 810}
    row["specifications"] = [{"name": "크기(높이x가로x깊이)", "value": "70~120cm x 1.6m x 80cm"}]
    metadata = product_metadata(row, "source")
    assert metadata["width_mm"] == 1600 and metadata["depth_mm"] == 800
    assert metadata["dimension_ranges_mm"]["height_mm"] == {"minimum": 700, "maximum": 1200}


def test_seat_dimensions_never_become_overall_width_and_total_height_is_preserved():
    row = {"product_id": "1", "name": "의자", "categories": [{"category_name": "식탁의자"}],
           "specifications": [{"name": "좌판가로", "value": "50cm"}, {"name": "좌판깊이", "value": "47.8cm"},
                              {"name": "좌판높이", "value": "44~54.6cm"}, {"name": "총높이", "value": "104.1cm"}]}
    metadata = product_metadata(row, "source")
    assert metadata["width_mm"] is metadata["depth_mm"] is None
    assert metadata["height_mm"] == 1041
    assert metadata["component_dimensions_mm"]["seat_width_mm"] == {"minimum": 500, "maximum": 500}
    assert metadata["component_dimensions_mm"]["seat_height_mm"] == {"minimum": 440, "maximum": 546}


def test_complete_spec_text_options_prevent_first_option_being_treated_as_selected():
    row = {"product_id": "1", "name": "상품", "specifications": [{"name": "색상", "value": "블랙"}],
           "spec_text": "의자 / 색상: 블랙, 화이트 / 재질: 원목 / 형태: 원형"}
    metadata = product_metadata(row, "source")
    assert metadata["color"] is None
    assert metadata["attribute_options"]["color"] == ["black", "white"]
    assert metadata["material"] == "wood" and metadata["shape"] == "round"
    assert "블랙, 화이트" in metadata["raw_attributes"]["색상"]
    row["spec_text"] = "색상: 검정 / 재질: 원목, 철제"
    metadata = product_metadata(row, "source")
    assert metadata["color"] == "black" and metadata["material"] is None
    assert metadata["attribute_options"]["material"] == ["metal", "wood"]


def test_component_material_and_shape_are_not_promoted_to_whole_product_attributes():
    row = {"product_id": "1", "name": "의자", "spec_text": "메쉬등판 / 패브릭좌판",
           "specifications": [{"name": "하부 프레임 형태", "value": "사각형"}]}
    metadata = product_metadata(row, "source")
    assert metadata["material"] is metadata["shape"] is None
    assert metadata["component_attributes"] == {"back_material": ["mesh"], "seat_material": ["fabric"], "frame_shape": ["rectangle"]}


def test_source_typo_and_conflicting_axes_are_retained_without_guessing_corrections():
    row = {"product_id": "1", "name": "책상", "categories": [{"category_name": "책상"}],
           "specifications": [{"name": "크기(가로x세로x높이)", "value": "1400x750x625~12750mm"},
                              {"name": "전체폭", "value": "150cm"}]}
    metadata = product_metadata(row, "source")
    assert metadata["width_mm"] is metadata["height_mm"] is None
    assert metadata["depth_mm"] == 750
    assert "height_mm" not in metadata["dimension_ranges_mm"]
    assert "1400x750x625~12750mm" in metadata["raw_attributes"]["크기(가로x세로x높이)"]
    assert any("requires_review" in issue for issue in metadata["metadata_issues"])
