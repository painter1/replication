import xml.dom.minidom

class KML(dict):

    def __init__(self):
        
        self['name'] = 'Noname'
        self['decription'] = ''
        self['styles'] = []
        self['placemarks'] = []
        self['lines'] = []

    def getXML(self):
        kml = self.__create_document()
        doc = kml.documentElement.getElementsByTagName('Document')[0]
        for item in self['styles']:
            s = self.__create_style(item)
            doc.appendChild(s.documentElement)
        for item in self['placemarks']:
            pl = self.__create_placemark(item)
            doc.appendChild(pl.documentElement)
        for item in self['lines']:
            l = self.__create_line(item)
            doc.appendChild(l.documentElement)

        return kml
                
    def __create_document(self):
        """Create the overall KML document."""
        doc = xml.dom.minidom.Document()
        kml = doc.createElement('kml')
        kml.setAttribute('xmlns', 'http://www.opengis.net/kml/2.2')
        doc.appendChild(kml)
        document = doc.createElement('Document')
        kml.appendChild(document)
        docName = doc.createElement('name')
        document.appendChild(docName)
        docName_text = doc.createTextNode(self['name'])
        docName.appendChild(docName_text)
        docDesc = doc.createElement('description')
        document.appendChild(docDesc)
        docDesc_text = doc.createTextNode(self['description'])
        docDesc.appendChild(docDesc_text)
        return doc

    def __create_placemark(self, item):
        """Generate the KML Placemark for a given item."""
        doc = xml.dom.minidom.Document()
        pm = doc.createElement("Placemark")
        doc.appendChild(pm)
        if 'style' in item:
            style_url = doc.createElement("styleUrl")
            pm.appendChild(style_url)
            style_url_text = doc.createTextNode('#%(style)s' % item)
            style_url.appendChild(style_url_text)
            
        name = doc.createElement("name")
        pm.appendChild(name)
        name_text = doc.createTextNode('%(name)s' % item)
        name.appendChild(name_text)
        desc = doc.createElement("description")
        pm.appendChild(desc)
        desc_text = doc.createTextNode(item.get('description', ''))
        desc.appendChild(desc_text)
        pt = doc.createElement("Point")
        pm.appendChild(pt)
        coords = doc.createElement("coordinates")
        pt.appendChild(coords)
        coords_text = doc.createTextNode('%(longitude)s,%(latitude)s,0' % item)
        coords.appendChild(coords_text)
        return doc

    def __create_line(self, item):
        """Generate the KML Placemark (line style) for a given item."""
        doc = xml.dom.minidom.Document()
        pm = doc.createElement("Placemark")
        doc.appendChild(pm)
        if 'style' in item:
            style_url = doc.createElement("styleUrl")
            pm.appendChild(style_url)
            style_url_text = doc.createTextNode('#%(style)s' % item)
            style_url.appendChild(style_url_text)
            
        name = doc.createElement("name")
        pm.appendChild(name)
        name_text = doc.createTextNode('%(name)s' % item)
        name.appendChild(name_text)
        desc = doc.createElement("description")
        pm.appendChild(desc)
        desc_text = doc.createTextNode(item.get('description', ''))
        desc.appendChild(desc_text)
        pt = doc.createElement("LineString")
        pm.appendChild(pt)
        coords = doc.createElement("coordinates")
        pt.appendChild(coords)
        point_str = ''
        for point in item['points']:
            point_str = point_str + '%(longitude)s,%(latitude)s,0 \t' % point
        coords_text = doc.createTextNode(point_str)
        coords.appendChild(coords_text)
        return doc
    def __create_style(self, style):
        """Create a new style for different placemark icons."""
        doc = xml.dom.minidom.Document()
        style_doc = doc.createElement('Style')
        style_doc.setAttribute('id', style['id'])
        doc.appendChild(style_doc)
        if 'icon' in style:
            icon_style = doc.createElement('IconStyle')
            style_doc.appendChild(icon_style)
            icon = doc.createElement('Icon')
            icon_style.appendChild(icon)
            href = doc.createElement('href')
            icon.appendChild(href)
            href_text = doc.createTextNode(style['icon']['href'])
            href.appendChild(href_text)
        if 'line' in style:
            line_style = doc.createElement('LineStyle')
            style_doc.appendChild(line_style)
            node = doc.createElement('color')
            node.appendChild(doc.createTextNode(style['line']['color']))
            line_style.appendChild(node)
            node = doc.createElement('width')
            node.appendChild(doc.createTextNode(style['line']['width']))
            line_style.appendChild(node)
            
        return doc

def example():
    to_dict = lambda **dd: dd
    kml = KML()
    kml['name'] = 'Test doc'
    kml['placemarks'].append({'name':'MPI-M','description':'blah', 'style':'type1','longitude':9,'latitude':10})
    kml['placemarks'].append({'name':'MPI-M','description':'blah', 'style':'type2','longitude':9,'latitude':10.1})
    kml['placemarks'].append({'name':'MPI-M','description':'blah', 'style':'type3','longitude':9,'latitude':10.2})
    k = kml.getXML()
    return k

def cmip5():
    to_dict = lambda **dd: dd
    kml = KML()
    kml['name'] = 'CMIP5 Archive Topology'
    kml['description'] = 'Beta'
    kml['styles'].append(to_dict(id='gateway',icon=to_dict(href='http://www.pdclipart.org/albums/Small_Icons/small_file_pull_2.png')))
    kml['styles'].append(to_dict(id='datanode',icon=to_dict(href='http://www.pdclipart.org/albums/Small_Icons/small_floppy_4.png')))
    kml['styles'].append(to_dict(id='institute',icon=to_dict(href='http://www.pdclipart.org/albums/Small_Icons/small_Bullet_computer_20.png')))
    kml['styles'].append(to_dict(id='datanode2gateway',line=to_dict(color='a3993333',width='5')))
    kml['styles'].append(to_dict(id='institute2datanode',line=to_dict(color='a3339933',width='3')))
    
    return kml

