
def year_sort(data_year_sequence):
   hist_dict, forecast_dict = get_historical_forecast_year_dict(data_year_sequence)
   hist_li = get_sorted_historical_and_forecast_year_li(hist_dict)
   forecast_li = get_sorted_historical_and_forecast_year_li(forecast_dict)
   return hist_li+forecast_li

def get_historical_forecast_year_dict(data_year_sequence):
    hist_dict = {}
    forecast_dict = {}
    for each_year in data_year_sequence:
        each = each_year    
        if not each:continue
        if 'E' == each[-1]:
            x = int(each[-5:-1])
            if not forecast_dict.get(x, {}):
                forecast_dict[x] = {} 
            y = each[:-5] 
            if not forecast_dict[x].get(y, []):
                forecast_dict[x][y] = []
            forecast_dict[x][y].append(each_year)
        else:
            try:
               x = int(each[-4:])
            except: continue
            if not hist_dict.get(x, {}):
                hist_dict[x] = {}
            y = each[:-4] 
            if not hist_dict[x].get(y, {}):
                hist_dict[x][y] = []
            hist_dict[x][y].append(each_year)
    return hist_dict, forecast_dict


def get_sorted_historical_and_forecast_year_li(tmp_dict):
    ks = tmp_dict.keys()
    ks.sort()
    qyear = ['Q1', 'Q2', 'Q3', 'Q4']
    hyear = ['H1', 'H2']
    fyear = ['FY']
    final_sort_years = []
    for k in ks:
        vs = tmp_dict[k]
        vks = vs.keys()
        if 'Q1' in vks or 'Q2' in vks or 'Q3' in vks or 'Q4' in vks:
            for vk in qyear:
                if tmp_dict[k].get(vk, {}):
                    final_sort_years += tmp_dict[k][vk]

        if 'H1' in vks or 'H2' in vks:
            for vk in hyear:
                if tmp_dict[k].get(vk, {}):
                    final_sort_years += tmp_dict[k][vk]

        if 'FY' in vks: 
            for vk in fyear:
                if tmp_dict[k].get(vk, {}):
                    final_sort_years += tmp_dict[k][vk]
    return final_sort_years

if __name__ == "__main__":
    li = ['Q12016', 'H12016', 'FY2016', "Q32016",'FY2016E','H12017E']
    print year_sort(li)
