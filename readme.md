### Methods
- [x] Add `drug_name` to pricing table using GPI-10 logic
    - see analysis_all_drugs.ipynb from logic


### Objective

- [ ] Universe as measured by 1 unit
- [ ] Simple count of number of unique prices
- [ ] Inpatient/ Outpatient distinction
- [ ] Gross price world
- [ ] Cash price world
- [ ] Negotiated cash price world
- [ ] Hospital measure of price variability their each of their own specific charge masters
- [ ] Simple count of the number of services where the cash discount is lower
- [ ] Disease state specific
- [ ] Drug specific example at the ASP billing unit level
- [ ] Random Forest model of 6 select products; Correlation matrix for variables (?)
- [ ] Hospital size / state / 340B status
- [ ] Inpatient/outpatient
- [ ] Number of insurers
- [ ] Region (NE/Midewest, South, West Coast)

### Discussion
    - Enforce standards on the books (10 years between ACA passage & 2019 rule) Billing standards



### Hospital Stats updated
- [x] 340B stats – how many hospitals in database are flagged as 340B Covered Entities
    - hospital.ipynb

- [x] Bed stats- min, max, average, median, standard deviation on bed characteristics
    - hospital.ipynb
### Product Universe Stats

- [x] How many unique products in database
    - analysis_all_drugs.ipynb
- [x] How many products per hospital on average (min, max, median, std of products per hospital)
    - hospitals.ipynb
- [ ] Break out universe of products into GPI-4 categories. Identify what categories are most represented by what we have vs. least. What percentage of each GPI-4 category total universe do we have pricing for (if there are 14 unique GPI values in a given GPI-4 (making it up) and we only have 2 we’d say for that one we have coverage of 2/14).
- [ ] Break out product coverage into 340B hospitals vs. none (are there appreciable differences in the products we see pricing for based upon 340B stats – thinking of like cancer drugs)
- [ ] For pricing universe information
- [ ] For pricing, calculate min/max ratio for each product at each unique pricing type on the specific hospital basis. The goal is to say, for hospital 1, the average ratio is X, the min is Y, the max is Z, the median is A, the std is B. We want to then build a database for all the hospitals to say, on average, hospitals have an average ratio of X, an average minimum ratio of Y, etc.
- [ ] Perform these (and what you’ve already done minus the current pricing attempt) product/pricing for all drug products, and then cancer, and then multiple sclerosis