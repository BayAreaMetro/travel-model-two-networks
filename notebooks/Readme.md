
### [01 Attribute Network and Make Scenario Pickle](01-attribute-and-make-pickles.ipynb)

Reads the standard network files for roadway and transit, and runs the following methods which calculate
attributes for the network; saves resulting pickled `Scenario`.

* [`mtc.determine_number_of_lanes()`](https://github.com/BayAreaMetro/Lasso/blob/e8c1d7db0a34e190d4592df3ab0758af2c04ad47/lasso/mtc.py#L173)
* [`mtc.calculate_facility_type()`](https://github.com/BayAreaMetro/Lasso/blob/e8c1d7db0a34e190d4592df3ab0758af2c04ad47/lasso/mtc.py#L23)
* [`mtc.calculate_useclass()`](https://github.com/BayAreaMetro/Lasso/blob/e8c1d7db0a34e190d4592df3ab0758af2c04ad47/lasso/mtc.py#L762)
* [`mtc.calculate_assignable()`](https://github.com/BayAreaMetro/Lasso/blob/e8c1d7db0a34e190d4592df3ab0758af2c04ad47/lasso/mtc.py#L426)
* [`mtc.calculate_transit()`](https://github.com/BayAreaMetro/Lasso/blob/e8c1d7db0a34e190d4592df3ab0758af2c04ad47/lasso/mtc.py#L668)
* [`mtc.calculate_farezone()`](https://github.com/BayAreaMetro/Lasso/blob/e8c1d7db0a34e190d4592df3ab0758af2c04ad47/lasso/mtc.py#L844)

Input:
* Standard roadway network, pre-base project cards (e.g. [`processed\version_12\standard_roadway_pre_base_project_cards`](https://mtcdrive.box.com/s/68zbn1dzz6d1uk6neiu4zfigj5a53ncs))
* Standard transit network, pre-base project cards (e.g. [`processed\verrsion_12\sandard_transit_pre_base_project_cards`](https://mtcdrive.box.com/s/m3z6yemj0o5ciyoyyiqeob6yu1ql70hm))

Output:
* Pickled [`Scenario`](https://github.com/BayAreaMetro/network_wrangler/blob/generic_agency/network_wrangler/scenario.py) instance containing the above networks with their new attributes, `working_scenario_01.pickle`

### [02 Apply all project cards](02-all-projects.ipynb)

Applies project cards to the standard network, to create a 2015 standard network. 

Applies project cards in sets based on tags:
1. 'highway review', 'Major Arterial Review', 'Reversible Lanes', 'Bus Only', 'Toll Plaza'
2. 'Managed Lanes', 'toll review', 'Exclude Trucks'
3. 'Major Transit links'
4. 'Minor Transit', 'Add Transit', 'Major Transit', 'Toll Plaza Transit'
5. 'External Stations Review'

To prepare for creating the model network, the following methods are then run:
1. [`mtc.add_tap_and_tap_connector()`]()
2. [`mtc.calculate_county()]()

Input: 
* Pickled [`Scenario`](https://github.com/BayAreaMetro/network_wrangler/blob/generic_agency/network_wrangler/scenario.py) instance output by [01 Attribute Network and Make Scenario Pickle](01-attribute-and-make-pickles.ipynb)

Output:
* Picked [`Scenario`](https://github.com/BayAreaMetro/network_wrangler/blob/generic_agency/network_wrangler/scenario.py) instance containting the networks with project cards applied, `working_scenario_vXX.pickle`
* Standard networks (e.g. `network_standard\vXX_\[link.json,node.geojson,shape.geojson\]`)

These can be converted to modeled networks via [convert_scenario_to_modeled_networks.py](../src/scripts/convert_scenario_to_modeled_networks.py)