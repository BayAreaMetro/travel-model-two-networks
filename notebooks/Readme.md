
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