# -*- coding: UTF-8 -*-
__author__ = 'Peter_Howe<haobibo@gmail.com>'

from pyxll import xl_func
from nlpir import Seg
from textmind.wenxin import process_paragraph, process_file, get_header

@xl_func("str paragraph: str",category="TextMind",thread_safe=True, volatile=False, allow_abort=True, disable_function_wizard_calc=True)
def Segment(paragraph):
    string = paragraph.decode('gbk').encode('utf-8')
    result = [term for term,pos in Seg(string)]
    return ' '.join(result).decode('utf-8').encode('gbk')

@xl_func("str paragraph: str[]",category="TextMind",thread_safe=True, volatile=False, allow_abort=True, disable_function_wizard_calc=True)
def Process_Paragraph(paragraph):
    try:
        string = paragraph.decode('gbk').encode('utf-8')
        r = process_paragraph(string,enable_pos=False)
        result = [str(i) for i in r.to_list(to_ratio=False)]
        return [result]
    except Exception as e:
        print(paragraph)
        print(e.message)
        return [['TextMind Program Error:',e.message]]

@xl_func("str file_path: str[]",category="TextMind",thread_safe=True, volatile=False, allow_abort=True, disable_function_wizard_calc=True)
def Process_File(file_path):
    for encode in ['utf-8-sig','gbk']:
        try:
            r = process_file(file_path,enable_pos=False,encoding=encode)
            result = [str(i) for i in r.to_list(to_ratio=False)]
            return [result]
        except Exception as e:
            continue

    return [['TextMind Program Error:',e.message]]

@xl_func(":string[]",category="TextMind",thread_safe=True, volatile=False, allow_abort=True, disable_function_wizard_calc=True)
def Get_Header():
    return [get_header(enable_pos=False)]

if __name__ == '__main__':
    string = r"　　好了，您已经成功地输出了对一个单元格的处理结果，接下来就可以利用Excel的“自动填充”功能来处理更多的文本了，而不需要重复上面的操作。如果您还不了解Excel的“自动填充”功能，我们建议您通过搜索引擎搜索该关键词，了解如何通过拖动单元格来重复公式。如果您已经熟悉“自动填充”功能，以下信息可能会对您有帮助："
    print Process_Paragraph(string.decode('utf-8').encode('gbk'))
    print Process_File(r"D:\Temp\20140324095815_ICTCLAS2014\test\18届三中全会.TXT".decode('utf-8'))