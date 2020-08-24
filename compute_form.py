#import utils.convert as convert
#con_obj = convert.Convert()
import utils.convert as scale_convert
sconvert_obj   = scale_convert.Convert()
import utils.numbercleanup as numbercleanup
numbercleanup_obj   = numbercleanup.numbercleanup()
import tavinash
import copy


class Compute():
    def __init__(self):
        self.ph_f_cond  = {}
        pass

    def parse_ph_formula(self, exprs):
        opr     = {
                    '=' : 1,
                    '+' : 1,
                    '-' : 1,
                    '*' : 1,
                    '/' : 1,
                    '(' : 1,
                    ')' : 1,
                    }
        f_ar    = []
        for expr in exprs:
            operands    = []
            tmp_ar      = []
            tph, op_str  = expr.split('=')
            for c in op_str:
                #print '\t',[c, c in opr]
                if c in opr:
                    if tmp_ar:
                        operands.append({'t':'op', 'v':''.join(tmp_ar)})
                        tmp_ar  = []
                    operands.append({'t':'opr', 'v':c})
                elif c:
                    tmp_ar.append(c)
            #print [tmp_ar]
            if tmp_ar:
                operands.append({'t':'op', 'v':''.join(tmp_ar)})
            #print operands
            if operands:
                formula = expr
                f_ar.append((formula, operands, formula))
        return f_ar

    def eval_ph_formula(self, row, form_ar, year, ptype, ph_map_d={}, clean_v_d={}, ph_gid_map={}, deal_id=None, table_type=None):
        for formula, ops, tmp_rowid in form_ar:
            expr_str    = []
            expr_val    = []
            f           = 0
            re_ar       = []
            #print '\n===================================================='
            scale_d     = {}
            fy          = 0
            fy_val      = 0
            pt_d        = {}
            nt_f        = 0
            for rr in ops:
                typ = rr['t']
                v   = rr['v']
                if typ == 'op':
                    pt_d[v] = 0
                    v   = v+year
                    if v not in ph_map_d:
                        expr_val.append('0')
                        continue
                    if ph_map_d[v] not in clean_v_d:
                        expr_val.append('0')
                        continue
                    v_d     = row[ph_map_d[v]]
                    phcsv   = v_d['phcsv']['s']
                    pt_d[rr['v']] = 1
                    if phcsv:
                        scale_d[phcsv]  = 1
            f_cond  = self.ph_f_cond.get(deal_id, {}).get(table_type, {}).get(formula, '')
            if not f_cond and ops and ops[0].get('cond'):
                f_cond  = ops[0].get('cond')
                
            if f_cond:
                texpr_str, texpr, tconver_arr    = self.form_expr_str(f_cond, year, clean_v_d, ph_map_d, row, '')
                o_t = eval(texpr_str)
                if not o_t:continue
            m_scale = ''
            if len(scale_d.keys()) > 1:
                scales  = map(lambda x:(sconvert_obj.num_obj[sconvert_obj.scale_map_d[x]], x), scale_d.keys())
                scales.sort(key=lambda x:x[0])
                m_scale = scales[-1][1]
                 
            expr_str    = []
            expr_val    = []
            f           = 0
            re_ar       = []
                
            conver_arr  = []
            scale_d     = {}
            form_ar     = []
            opr_f       = '+'
            for rr in ops:
                typ = rr['t']
                v   = rr['v']
                expr_str.append(v)
                if typ == 'opr':
                    opr_f       = v
                    expr_val.append(v)
                elif typ == 'cons':
                    expr_val.append(v)
                elif typ == 'op':
                    v   = v+year
                    #print [v, ph_map_d.get(v), clean_v_d.get(ph_map_d.get(v))]
                    if v in row:
                        ph_map_d[v] = v
                    if v not in ph_map_d:
                        expr_val.append('0')
                        #f           = 0
                        #break
                        form_ar.append({'txid':str(row['t_id']), 'op':opr_f, 'ph':v, 'pk':''})
                        continue
                    if ph_map_d[v] not in clean_v_d:
                        v_d     = row[ph_map_d[v]]
                        cvalue  = numbercleanup_obj.get_value_cleanup(v_d['v'])
                        if cvalue:
                            clean_v_d[ph_map_d[v]]  = cvalue
                        
                    if ph_map_d[v] not in clean_v_d:
                        form_ar.append({'txid':str(row['t_id']), 'op':opr_f, 'ph':v, 'pk':ph_map_d[v]})
                        expr_val.append('0')
                        #f           = 0
                        #break
                        continue
                    #print rr, ph_map_d[v]
                    v_d     = row[ph_map_d[v]]
                    #if v_d['x'] and v_d['bbox'] and v_d['t'] and v_d['d']:
                    re_ar.append({'i':len(re_ar), 'x':v_d['x'], 'bbox':v_d['bbox'], 't':v_d.get('t', ''), 'd':v_d['d'], 'phcsv':v_d['phcsv'], 'FORM_VALUE':v_d.get('FORM_VALUE', ''), 'page_no':v_d.get('page_no', ''), 'p_coord':v_d.get('p_coord', '')})
                    form_ar.append({'txid':str(row['t_id']), 'op':opr_f, 'ph':v, 'pk':ph_map_d[v]})
                    if clean_v_d[ph_map_d[v]] == '':
                        expr_val.append('0')
                    else:
                        f = 1
                        #print v_d
                        if m_scale:
                            phcsv   = v_d['phcsv']['s']
                            if phcsv != m_scale:
                                tv, factor  = sconvert_obj.convert_frm_to(phcsv, m_scale, value)
                                if factor:
                                    conver_arr.append((clean_v_d[ph_map_d[v]], tv, '%s-%s'%(phcsv, m_scale)))
                                    expr_val.append(str(tv))
                                else:
                                    expr_val.append(str(value))
                            else:
                                scale_d[m_scale]    = re_ar[-1]
                                expr_val.append(clean_v_d[ph_map_d[v]])
                        else:
                            expr_val.append(clean_v_d[ph_map_d[v]])
            #print f, ''.join(expr_val)
            if f == 0:
                continue
            v   = ''
            try:
                v   = str(eval(''.join(expr_val)))
            except:
                continue
            if v == '' or v == 0.0:continue
            v    = sconvert_obj.convert_floating_point(v)
            #print [v]
            re_ar.sort(key=lambda v_d: (v_d['i'], 0 if v_d['x'] and v_d['bbox'] and v_d['t'] and v_d['d'] else 1 ))
            if re_ar and (m_scale in scale_d):
                ref_d   = scale_d[m_scale]
            elif re_ar:
                ref_d   = re_ar[0]
            else:
                ref_d   = {}
            ref_d['expr_str']   = ''.join(expr_str)+' -- '+str(conver_arr)
            ref_d['expr_val']   = ''.join(expr_val)
            ref_d['fv']         = 'Y'
            ref_d['PH_D']       = 'Y'
            ref_d['PH_FORM']    = form_ar
            ref_d['PH_FORM_STR']    = formula
            ref_d['v']          = v
            if tmp_rowid == 'RID-0':
                return v, ref_d, str(ph_gid_map.get(formula, ''))+'~'+tmp_rowid
            else:
                return v, ref_d, ph_gid_map.get(formula, '')
        return '', {}, ''

    def ph_derivation(self, row, ph, expr):
        form_ar = self.parse_ph_formula(expr)
        year    = ph[-4:]
        ptype   = ph[:-4]
        v, ref_d, g_id  = self.eval_ph_formula(row, form_ar, year, ptype, ph_map_d={}, clean_v_d={}, ph_gid_map={}, deal_id=None, table_type=None)
        return v, ref_d


    ## EVALUATION 
    def get_formula_evaluation(self, formula, taxo_id_dict, phs, ph_map_d={}, year=None, run_all=None, sys_op_d={},keep_org=None, exist_sign_d={}):
        res     = {} #formula[0]
        opers   = [] #formula[1:]
        for rr in formula:
            if rr['op'] == '=':
                res = rr
            else:
                opers.append(rr)
        res_taxo_id = str(res['txid'])
        flip_sign   = {}

        def get_eval(taxo_id, ph, res_val, sys_op_inds, re_scale):
            #print [taxo_id, ph, res_val]
            val_li      = []
            res_txid    = res['txid']
            #res_tt      = res['t_type']
            re_ar       = []
            expr_str    = []
            #print '\n========================'
            op_inds     = []
            scale_d     = {}
            v_ar        = []
            val_d       = {}
            for oper in opers:
                op_txid    = oper['txid']
                op         = oper['op']
                op_type    = oper['type']
                tph         = ph
                if oper.get('k', ''):
                    tph  = oper['k']
                elif oper.get('period', ''):
                    tph  = ph_map_d.get(oper['period']+str(int(year)+int(oper['yd'])), '')
                if op_type == 'v':
                    pass
                else:
                    op_val = taxo_id_dict.get(op_txid, {}).get(tph, {'v':0})['v']
                    clean_value = op_val
                    try:
                        clean_value = numbercleanup_obj.get_value_cleanup(clean_value)
                    except:clean_value = ''
                    if clean_value == '':
                        clean_value = '0'
                    phcsv   = taxo_id_dict.get(op_txid, {}).get(tph, {}).get('phcsv', {}).get('s', '')
                    val_d[(op_txid, tph)]       = (clean_value, phcsv)
                    clean_value = float(clean_value)
                    v_ar.append(clean_value)
                    if phcsv:
                        scale_d[phcsv]  = 1
            m_scale = ''
            if len(scale_d.keys()) > 1:
                if not re_scale:
                    scales  = map(lambda x:(sconvert_obj.num_obj.get(sconvert_obj.scale_map_d.get(x, ''), 9999), x), scale_d.keys())
                    scales.sort(key=lambda x:x[0])
                    m_scale = scales[-1][1]
                else:
                    m_scale = re_scale
            elif re_scale:
                m_scale = re_scale

            
            v_ar        = []
            for oper in opers:
                op_txid    = oper['txid']
                op         = oper['op']
                op_type    = oper['type']
                tph         = ph
                if oper.get('k', ''):
                    tph  = oper['k']
                elif oper.get('period', ''):
                    tph  = ph_map_d.get(oper['period']+str(int(year)+int(oper['yd'])), '')
                if op_type == 'v':
                    pass
                else:
                    op_val = taxo_id_dict.get(op_txid, {}).get(tph, {'v':0})['v']
                    clean_value, phcsv  = val_d[(op_txid, tph)]
                    if clean_value and m_scale:
                        if phcsv and  m_scale != phcsv:
                            tv  = sconvert_obj.convert_frm_to(phcsv, m_scale, clean_value)
                            clean_value = tv.replace(',', '')
                    if clean_value == '':
                        clean_value = '0'
                    clean_value = float(clean_value)
                    v_ar.append(clean_value)

            if res_val and keep_org != 'N':
                v_ar.append(float(res_val))
                print v_ar
                vtup    = tuple(v_ar)
                op_inds = []
                if vtup not in exist_sign_d:
                    f_sig   = tavinash.get_sig(v_ar)
                    print 'f_sig ', f_sig
                    if f_sig:
                        op_inds = map(lambda x:'+' if x==0 else '-', f_sig[0])
                        exist_sign_d[vtup]  = op_inds
                else:
                    op_inds = exist_sign_d[vtup]
                if not op_inds and sys_op_inds:
                    op_inds = sys_op_inds
                if op_inds:
                    tmp_v_ar    = []
                    for i, v1 in enumerate(v_ar[:-1]):
                        op  = op_inds[i]
                        if op == '-':
                            v1  = -1*v1
                        tmp_v_ar.append(v1)
                    print '\n'
                    print v_ar, tmp_v_ar
                    print v_ar[-1], sum(tmp_v_ar), abs(v_ar[-1]-sum(tmp_v_ar)) > 0
                    v_ar    = tmp_v_ar[:] #+[float(res_val)]
                else:
                    v_ar    = v_ar[:-1]
            tv_ar   = copy.deepcopy(v_ar)
            #print '\t\t',[m_scale, scale_d.keys()]
            

            n_form  = [copy.deepcopy(res)]
            op_ind  = 0
            scale_d = {}
            conver_arr  = []
            ftype   = 'G'
            for oper in opers:
                #print oper, op_inds, sys_op_inds
                op_txid    = oper['txid']
                op         = oper['op']
                #op_tt      = oper['t_type']
                op_type    = oper['type']
                tph         = ph
                s_ph        = ph 
                if oper.get('k', ''):
                    tph  = oper['k']
                elif oper.get('period', ''):
                    #print '\tTT',[oper['period'],year,  oper['yd']]
                    s_ph    = oper['period']+str(int(year)+int(oper['yd']))
                    tph  = ph_map_d.get(oper['period']+str(int(year)+int(oper['yd'])), '')
                #print [tph]
                desc        = ''
                v_f         = ()
                if op_type == 'v':
                    op_val  = op_txid
                    desc    = op_txid
                    if op not in {'+':1, '-':1}:
                        ftype   = 'NG'
                else:
                    if op_inds:
                        op  = op_inds[op_ind]
                    if op not in {'+':1, '-':1}:
                        ftype   = 'NG'
                    op_ind  += 1
                    op_val = taxo_id_dict.get(op_txid, {}).get(tph, {'v':0})['v']
                        
                    desc    = taxo_id_dict.get(op_txid, {'t_l':''})['t_l']
                    #print [desc, op_val]
                    if taxo_id_dict.get(op_txid, {}).get(tph, {}).get('x', None) != None:
                        v_d = taxo_id_dict.get(op_txid, {}).get(tph, {})
                        re_ar.append({'i':len(re_ar), 'x':v_d['x'], 'bbox':v_d.get('bbox', []), 't':v_d.get('t', ''), 'd':v_d['d'], 'phcsv':copy.deepcopy(v_d.get('phcsv', {})), 'page_no':v_d.get('page_no', ''), 'p_coord':v_d.get('p_coord', '')})
                        clean_value, phcsv  = val_d[(op_txid, tph)]
                        if clean_value:
                            v_f         = (op_txid, clean_value, tph)

                        if phcsv and m_scale:
                            if clean_value == '':
                                clean_value = '0'
                            if phcsv != m_scale:
                                tv, factor   = sconvert_obj.convert_frm_to_1(phcsv, m_scale,clean_value)
                                if factor:
                                    conver_arr.append((clean_value, tv, '%s-%s'%(phcsv, m_scale)))
                                    op_val  = str(tv)
                            else:
                                scale_d[m_scale]    = re_ar[-1]
                            
                oper['op']  = op
                oper['ph']  = s_ph
                oper['pk']  = tph
                n_form.append(copy.deepcopy(oper))
                #print [op_val]
                clean_value = op_val
                try:
                    clean_value = numbercleanup_obj.get_value_cleanup(clean_value)
                except:clean_value = ''
                if op == '-':
                    if v_f:
                        if '-' in v_f[1]:
                            flip_sign.setdefault(v_f[0], {})[v_f[2]]    = 1
                        elif v_f[1]:
                            flip_sign.setdefault(v_f[0], {})[v_f[2]]    = 1

                if clean_value == '':
                    clean_value = '0'
                expr_str += [str(op), desc] 
                val_li += [str(op), str(clean_value)]
            cell_val = 0
            #print [''.join(val_li)]
            try:
                cell_val = eval(''.join(val_li))
            except:pass
            re_ar.sort(key=lambda v_d: (v_d['i'], 0 if v_d['x'] and v_d['bbox'] and v_d['t'] and v_d['d'] else 1 ))
            if not re_ar or cell_val == 0:
                return '', {}, [], [], n_form, []
            if re_ar and (m_scale in scale_d):
                re_ar   = [scale_d[m_scale]]
            cell_val    = str(cell_val)
            #print '\n==================='
            #print [cell_val, ''.join(val_li)]
            cell_val    = sconvert_obj.convert_floating_point(cell_val)
            if cell_val == '' or cell_val == '0':
                return '', {}, [], [], n_form, []
            cell_val    = cell_val.replace(',', '')
            dd          = copy.deepcopy(re_ar[0])
            dd['v']     = str(cell_val)
            dd['v_ar']  = tv_ar
            dd['ftype']  = ftype
            #print 'END', [str(cell_val), dd, val_li, expr_str]
            n_form[0]['conver_arr'] = conver_arr
            return str(cell_val), dd, val_li, expr_str, n_form, conver_arr
           
        val_dict = {}
        form_d  = {}
        #print '\n===================================='
        #print taxo_id_dict[res_taxo_id]['t_l']
        for ph in phs:
            if taxo_id_dict[res_taxo_id].get(ph, {}).get('v', '') == '' or run_all=='Y':
                clean_value = taxo_id_dict[res_taxo_id].get(ph, {}).get('v', '')
                try:
                    clean_value = numbercleanup_obj.get_value_cleanup(clean_value)
                except:clean_value = ''
                #print '\t', [ph, clean_value, taxo_id_dict[res_taxo_id].get(ph, {}).get('phcsv', {}).get('s', '')]
                cell_val, ref, expr_val, expr_str, n_form, conver_arr = get_eval(res_taxo_id, ph, clean_value, sys_op_d.get((str(res_taxo_id), ph), []), taxo_id_dict[res_taxo_id].get(ph, {}).get('phcsv', {}).get('s', ''))
                form_d[ph]  = n_form
                if cell_val not in ['0', '']:
                    ref['v'] = cell_val
                    ref['fv'] = 'Y'
                    ref['expr_str'] = ''.join(expr_str)+' '+str(conver_arr)
                    ref['expr_val'] = ''.join(expr_val)
                    val_dict.setdefault(res_taxo_id, {})[ph] = ref
        return val_dict, form_d, flip_sign

    def col_formula(self, phs, taxo_id_dict, formula, run_all=None, keep_org=None, op_d={}, exist_sign_d={}):
        val_dict, form_d, flip_sign  = self.get_formula_evaluation(formula, taxo_id_dict, phs, {}, None, run_all, op_d, keep_org, exist_sign_d)
        return val_dict, form_d, flip_sign


            
    
