# public states_dict

"""
    states_dict
A Dictionary of States/UTs that can appear in the data set. The keys will be returned in the cleaning steps, and the values can be matched in the underlying datasets.
"""
states_dict = Dict(
    [
        ["A&N Island"; "A&N Islands"; "Andaman and Nicobar Islands"; "Andaman and Nicobar"; "Andaman"] .=> "A&N Island";
        ["Andhra Pradesh"; "AP"] .=> "Andhra Pradesh";
        ["Arunachal Pradesh"; "AR"] .=> "Arunachal Pradesh";
        ["Assam"] .=> "Assam";
        ["Bihar"] .=> "Bihar";
        ["Chandigarh"] .=> "Chandigarh";
        ["Chhattisgarh"] .=> "Chhattisgarh";
        ["Dadra and Nagar Haveli and Daman and Diu"] .=> "Dadra and Nagar Haveli and Daman and Diu";
        ["Delhi"] .=> "Delhi";
        ["Goa"] .=> "Goa";
        ["Gujarat"] .=> "Gujarat";
        ["Haryana"] .=> "Haryana";
        ["Himachal Pradesh"; "HP"] .=> "Himachal Pradesh";
        ["Jammu & Kashmir"; "Jammu Kashmir"; "Jammu and Kashmir"] .=> "Jammu & Kashmir";
        ["Jharkhand"] .=> "Jharkhand";
        ["Karnataka", "Kamataka"] .=> "Karnataka";
        ["Kerala"] .=> "Kerala";
        ["Madhya Pradesh"; "MP"] .=> "Madhya Pradesh";
        ["Maharashtra"] .=> "Maharashtra";
        ["Manipur"] .=> "Manipur";
        ["Meghalaya"] .=> "Meghalaya";
        ["Mizoram"] .=> "Mizoram";
        ["Nagaland"] .=> "Nagaland";
        ["Odisha"] .=> "Odisha";
        ["Pondicherry"; "Pondichery"; "Pudhucherry"; "Puducherry"] .=> "Pondichery";
        ["Punjab"] .=> "Punjab";
        ["Rajasthan"] .=> "Rajasthan";
        ["Sikkim"] .=> "Sikkim";
        ["Tamil Nadu"; "Tamilnadu"] .=> "Tamil Nadu";
        ["Telangana"; "Telanagana"] .=> "Telangana";
        ["Tripura"] .=> "Tripura";
        ["Uttar Pradesh"; "UP"] .=> "Uttar Pradesh";
        ["Uttarakhand"] .=> "Uttarakhand";
        ["West Bengal"] .=> "West Bengal";
    ]
)

state_code_dict = Dict(
    "A&N Island" => "AN",
    "Andhra Pradesh" => "AP",
    "Arunachal Pradesh" => "AR",
    "Assam" => "AS",
    "Bihar" => "BR",
    "Chandigarh" => "CH",
    "Chhattisgarh" => "CG",
    "Dadra and Nagar Haveli and Daman and Diu" => "DD",
    "Goa" => "GA",
    "Gujarat" => "GJ",
    "Haryana" => "HR",
    "Himachal Pradesh" => "HP",
    "Jammu & Kashmir" => "JK",
    "Jharkhand" => "JH",
    "Karnataka" => "KA",
    "Kerala" => "KL",
    "Madhya Pradesh" => "MP",
    "Maharashtra" => "MH",
    "Manipur" => "MN",
    "Meghalaya" => "ML",
    "Mizoram" => "MZ",
    "Nagaland" => "NL",
    "Odisha" => "OD",
    "Pondichery" => "PY",
    "Punjab" => "PB",
    "Rajasthan" => "RJ",
    "Sikkim" => "SK",
    "Tamil Nadu" => "TN",
    "Tripura" => "TR",
    "Telangana" => "TS",
    "Uttar Pradesh" => "UP",
    "Uttarakhand" => "UK",
    "West Bengal" => "WB"
)
