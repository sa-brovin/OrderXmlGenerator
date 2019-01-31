# -*- coding: utf-8 -*-
import datetime
import uuid as u
import paramiko
from lxml import etree, objectify
from random import randint

c_server = "192.168.111.32"
c_port = 22
c_user = "abrovin"
c_password = ""
c_order_file_name = "Order_%s.xml" % datetime.datetime.now().isoformat()
c_exchange_folder = "/var/lib/aks-adapter-sap/exchange/soap"


def create_ssh_client(server, port, user, password):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(server, port, user, password)
    return client


# region create xml nodes


def create_e1afvol(data):
    appt = objectify.Element("E1AFVOL")
    appt.VORNR = data["VORNR"]
    appt.LTXA1 = data["LTXA1"]
    appt.ARBID = data["ARBID"]
    appt.MGVRG = data["MGVRG"]
    appt.MEINH = data["MEINH"]
    appt.LMNGA = data["LMNGA"]
    return appt


def create_e1jstkl(data):
    appt = objectify.Element("E1JSTKL")
    appt.STAT = data["STAT"]
    return appt


def create_e1afpol(data):
    appt = objectify.Element("E1AFPOL")
    appt.VERID = data["VERID"]
    return appt


def create_appt(data):
    appt = objectify.Element("Item")
    appt.AUFNR = data["AUFNR"]
    appt.AUART = data["AUART"]
    appt.GSTRS = data["GSTRS"]
    appt.GLTRS = data["GLTRS"]
    appt.LGORT = data["LGORT"]
    appt.PLNBEZ = data["PLNBEZ"]
    appt.MATXT = data["MATXT"]
    appt.HF_DSE_NAME = data["HF_DSE_NAME"]
    appt.HF_DSE_KTD = data["HF_DSE_KTD"]
    appt.HF_DSE_NAME_H = data["HF_DSE_NAME_H"]
    appt.BMENGE = data["BMENGE"]
    appt.BMEINS = data["BMEINS"]
    appt.IGMNG = data["IGMNG"]
    appt.APRIO = data["APRIO"]
    return appt


# endregion


def create_xml():
    # Создаем XML файл.
    xml = '''<SOAP:Envelope xmlns:SOAP='http://schemas.xmlsoap.org/soap/envelope/'>
        <SOAP:Header/>
        <SOAP:Body>
        <ProductionOrderRequest xmlns:prx='urn:sap.com:proxy:EPR:/1SAI/TAS6CA1BE8F0794C74DF05D:731'>
        <HeaderSection>
        <UUID>{0}</UUID>
        <Sender>KSAUPKK_ERP</Sender>
        <Recipients>SUIKP</Recipients>
        <CreationDateTime>{1}</CreationDateTime>
        </HeaderSection>
        <DataSection>
        </DataSection>
        </ProductionOrderRequest>
        </SOAP:Body>
        </SOAP:Envelope>
    '''.format(u.uuid1(), "%sZ" % datetime.datetime.now().isoformat(timespec="seconds"))

    root = objectify.fromstring(xml)
    nav = root.find(".//DataSection")

    # заполнение данных
    order_name = "I_{0}_{1}".format(datetime.datetime.now().strftime("%Y-%m-%d"), randint(1, 1000))
    manufacture = 101
    # sector = '01'
    sector_hex = "{0:x}".format(1)

    component_id = "component_id"
    full_name = "full_name"
    plain_name = "plain_name"
    code = "code"
    stage = "stage"
    verid = "verid"

    operation_code = "operation_code"
    operation_name = "operation_name"
    tool_ext_id = "tool_ext_id"
    amount = 777

    item = create_appt({
        "AUFNR": order_name,
        "AUART": "P%s" % manufacture,
        "GSTRS": datetime.datetime.now().strftime("%Y-%m-%d"),
        "GLTRS": datetime.datetime.now().strftime("%Y-%m-%d"),
        "LGORT": "{0}{1}".format(manufacture, sector_hex),

        "PLNBEZ": component_id,
        "MATXT": full_name,
        "HF_DSE_NAME": plain_name,
        "HF_DSE_KTD": code,
        "HF_DSE_NAME_H": stage,
        "BMENGE": amount,
        "BMEINS": "ST",
        "IGMNG": "0.0",
        "APRIO": "",
    })
    nav.append(item)

    # вариант изготовления ДСЕ
    eiafpol = create_e1afpol({"VERID": verid})
    nav = root.find(".//APRIO")
    nav.addnext(eiafpol)

    # статус заказа
    e1jstkl = create_e1jstkl({"STAT": "I0002"})
    nav = root.find(".//E1AFPOL")
    nav.addnext(e1jstkl)

    # операция по заказу
    e1afvol = create_e1afvol({
        "VORNR": operation_code,
        "LTXA1": operation_name,
        "ARBID": tool_ext_id,
        "MGVRG": amount,
        "MEINH": "ST",
        "LMNGA": "0.0",

    })
    nav = root.find(".//E1JSTKL")
    nav.addnext(e1afvol)

    # удаляем все lxml аннотации.
    objectify.deannotate(root)
    etree.cleanup_namespaces(root)

    obj_xml = etree.tostring(root,
                             encoding="UTF-8",
                             pretty_print=True,
                             xml_declaration=True
                             )

    try:
        with open(c_order_file_name, "wb") as xml_writer:
            xml_writer.write(obj_xml)
    except IOError:
        pass

    # копирование файла в директорию aks-sap-adapter для обмена
    # ssh = create_ssh_client(c_server, c_port, c_user, c_password)
    # scp = SCPClient(ssh.get_transport())
    # scp.put(c_order_file_name, c_exchange_folder, False)


def main():
    create_xml()


if __name__ == '__main__':
    main()

# print(xml)
