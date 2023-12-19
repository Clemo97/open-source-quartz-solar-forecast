"""
Forecasts

v1: This model is trained with pv-site-predictions - https://github.com/openclimatefix/pv-site-prediction
The model is a gradient boosted tree model and uses 9 NWP variables from the UK MetOffice.
It is trained on 25,000 PV sites with over 5 years of PV history, which is available [here](https://huggingface.co/datasets/openclimatefix/uk_pv).

"""