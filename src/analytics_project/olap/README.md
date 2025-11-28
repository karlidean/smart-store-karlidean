# Project 6 - OLAP Analysis - Total Customer Purchases by Product

## Methods and Introduction
**Question:** How many products is each customer purchasing? Can we find out how many of each product is being purchased in the process?\
\
**Purpose:** This could be an entry-level analysis to understanding a store's stock levels and what a customer in this store finds valuable.

## Data
In previous projects, we have prepared a data warehouse and a star/relational schema. I will be using this data warehouse for my analysis. You can find this data [here](link to prepared folder). This data warehouse is repeatable in us being able to update our raw data files (found [here](link to raw data)), so the data can be re-cleaned and standardized before entering the data warehouse.

## Tools I Used
In previous projects (P3-P5) I used much of Python and SQL, but now we will be using PowerBI as we have implemented the relational warehouse last project into the PowerBI system.

## Workflow and Logic (Post - Project Setup in VS Code)
### Slicing (Filtering by 1 Dimension to Isolate that Metric's Performance)
**Objective:** Determine the amount of sales brought in by a single customer per item.\
**Methodology:** Set up a matrix with the product's names and cetegories, tallying the final sale amount. I filtered by the customer named "Ashley Todd".
### Dicing (Filtering by 2 Cross-Compatible Dimensions to Show Comparative Performance Metrics)
**Objective:** Determine the types of members purchasing certain item categories.\
**Methodology:** Create a stacked bar chart of total sales with categories and member statuses, with slicers of regions and supplier names.
### Drill-Down (Creating a Heirarchy and Defining Deep Insights)
**Objective:** Determine how many sales per product we are receiving per supplier.\
**Methodology:** Create a product hierarchy to understand the product specifications, and use column charts to help define and drill down into the most valuable category and supplier. Legend by the member's status to help understand what kinds of customers are valuing what product specifically.

## Results
### Slicing
Ashley Todd had spent $9,290.54 in her time with us as a customer. Her highest valued and priced category was the "Home" product category. She loved the "Home-Year" items, where she purchased $2,774.86 worth ot items. \
You can find this matrix [here](link to png in folder).

### Dicing
Breaking down by region, we were able to see what membership levels are purchasing from certain suppliers. We were able to see that our more active members are of higher tiers on average. However, the Gold Tier Membership are the customers getting the most out of their membership, especially in the Central Region. They blow every other member tier out of the water when it comes to count of sales. Looking at sales from Pinball Wizard Electronics, we can see clearly that it is a fan-favorite among our Gold Members! \
You can find this chart [here](link to png in folder.)

### Drilldown
In our drilldown, we were able to see what kinds of customers liked certain suppliers, but now we can see how many purchases for each product and by what kind of member. To look further into this, I drilled into the our most purchased from category, "home", and further into our most purchased from supplier in that category, Country Farm Homemakers. We can now see a breakout of the types of customers purchasing different items. We can see that the "Term" nd "Young" items are very comparable in amount of sales, also being a fan-favorite of the Diamond Members! \
You can find this chart [here](link to png in folder).

## Suggested Business Action

## Challenges
