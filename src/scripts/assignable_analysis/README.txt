Assignable analysis overview:

 - The purpose of this analysis is to set an "ASSIGNABLE" field on network links.
 Any links without ASSIGNABLE = 1 can be removed from highway assignment to  decrease run time.

 Assignable is assigned done in several steps
 1. Run the full network through assignment without filtering out the ASSIGNABLE links.
 2. Perform time and distance assignments on the full network with a scalar demand matrix to all TAZ pairs.
 3. Set ASSIGNABLE = 1 for all links that have volume from any of the assignments done in steps 1 and 2.

How to Run:
 - Run TM2 assignment without filtering on ASSIGNABLE link (check the CreateFiveHighwayNetworks.job script to ensure no filtering is done)
 - Output network from that assignment is the input to this analysis
 - Check file names and paths in the assign_scalar.s script
 - Run the run_assignable.bat batch file

 Future enhancements:
  - Include links that have transit operating on them (first pass at the code is included in the add_transit.s script)
