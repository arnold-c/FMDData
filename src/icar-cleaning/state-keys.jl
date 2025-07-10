# public states_dict

"""
    states_dict
A Dictionary of States/UTs that can appear in the data set. The keys will be returned in the cleaning steps, and the values can be matched in the underlying datasets.
"""
states_dict = Dict(
    [
        ["A&N Island"; "A&N Islands"; "Andaman and Nicobar Islands"] .=> "A&N Island";
        ["Andaman"] .=> "Andaman";
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
        ["Jammu & Kashmir"; "Jammu Kashmir"] .=> "Jammu & Kashmir";
        ["Jharkhand"] .=> "Jharkhand";
        ["Kamataka"] .=> "Kamataka";
        ["Karnataka"] .=> "Karnataka";
        ["Kerala"] .=> "Kerala";
        ["Madhya Pradesh"; "MP"] .=> "Madhya Pradesh";
        ["Maharashtra"] .=> "Maharashtra";
        ["Manipur"] .=> "Manipur";
        ["Meghalaya"] .=> "Meghalaya";
        ["Mizoram"] .=> "Mizoram";
        ["Nagaland"] .=> "Nagaland";
        ["Odisha"] .=> "Odisha";
        ["Pondicherry"; "Pondichery"; "Pudhucherry"] .=> "Pondichery";
        ["Punjab"] .=> "Punjab";
        ["Rajasthan"] .=> "Rajasthan";
        ["Sikkim"] .=> "Sikkim";
        ["Tamil Nadu"; "Tamilnadu"] .=> "Tamil Nadu";
        ["Telangana"; "Telanagana"] .=> "Telangana";
        ["Uttar Pradesh"; "UP"] .=> "Uttar Pradesh";
        ["Uttarakhand"] .=> "Uttarakhand";
        ["West Bengal"] .=> "West Bengal";
    ]
)
